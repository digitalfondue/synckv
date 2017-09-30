package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.SyncKV.SyncKVTable;
import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.h2.mvstore.MVMap;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RpcFacade {

    private final static Logger LOGGER = Logger.getLogger(RpcFacade.class.getName());

    private final SyncKV syncKV;
    private final Map<Address, List<TableMetadata>> syncPayloads;
    private final AtomicLong lastDataSync = new AtomicLong();
    private RpcDispatcher rpcDispatcher;

    RpcFacade(SyncKV syncKV) {
        this.syncKV = syncKV;
        this.syncPayloads = syncKV.syncPayloads;
    }

    private Address getCurrentAddress() {
        return syncKV.channel.getAddress();
    }

    private boolean canIgnoreMessage() {
        return System.currentTimeMillis() - lastDataSync.get() <= 10 * 500;
    }

    public void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
        this.rpcDispatcher = rpcDispatcher;
    }

    //-----------

    void putRequest(Address source, String table, String key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall("handlePutRequest", new Object[]{Utils.addressToBase64(source), table, key, value}, new Class[]{String.class, String.class, String.class, byte[].class}));
    }

    public void handlePutRequest(String src, String table, String key, byte[] value) {

        Address source = Utils.fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncKV.getTable(table).put(key, value, false);
    }

    //-----------

    void syncPayloadFrom(Address address, List<TableAddress> remote) {
        send(address, new MethodCall("handleSyncPayloadFrom", new Object[]{remote}, new Class[]{List.class}));
    }

    public void handleSyncPayloadFrom(List<TableAddress> remote) {
        if (canIgnoreMessage()) {
            return;
        }

        Map<String, List<TableAddress>> addressesAndTables = new HashMap<>();
        remote.stream().forEach(ta -> {
            if (!addressesAndTables.containsKey(ta.addressEncoded)) {
                addressesAndTables.put(ta.addressEncoded, new ArrayList<>());
            }
            addressesAndTables.get(ta.addressEncoded).add(ta);
        });

        addressesAndTables.forEach((encodedAddress, tables) -> {
            Address target = Utils.fromBase64(encodedAddress);
            List<TableMetadata> tableMetadata = new ArrayList<>();
            Set<String> fullSync = new HashSet<>();
            tables.forEach(t -> {
                tableMetadata.add(syncKV.getTableMetadata(t.table));
                if (t.fullSync) {
                    fullSync.add(t.table);
                }
            });
            syncPayload(target, getCurrentAddress(), tableMetadata, fullSync);
        });
    }

    //-----------

    void dataToSync(Address address, String name, Map<String, PayloadAndTime> payload) {
        send(address, new MethodCall("handleDataToSync", new Object[]{name, payload}, new Class[]{String.class, Map.class}));
    }

    public void handleDataToSync(String name, Map<String, PayloadAndTime> payload) {
        SyncKV.SyncKVTable table = syncKV.getTable(name);
        payload.forEach((k, v) -> {
            if (!table.present(k, v.payload) || table.isNewer(k, v.time)) {
                table.put(k, v.payload, false);
            }
        });
        syncKV.commit();
        lastDataSync.set(System.currentTimeMillis());
    }

    //-----------


    void requestForSyncPayload(Address address) {
        broadcastToEverybodyElse(new MethodCall("handleRequestForSyncPayload", new Object[]{Utils.addressToBase64(address)}, new Class[]{String.class}));
    }

    public void handleRequestForSyncPayload(String src) {

        if (canIgnoreMessage()) {
            return;
        }

        Address leader = Utils.fromBase64(src);

        if (leader.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncPayloadForLeader(leader, getCurrentAddress(), syncKV.getTableMetadataForSync());

    }

    //-----------

    void syncPayload(Address addressToSend, Address address, List<TableMetadata> metadata, Set<String> fullSync) {
        send(addressToSend, new MethodCall("handleSyncPayload", new Object[]{Utils.addressToBase64(address), metadata, fullSync}, new Class[]{String.class, List.class, Set.class}));
    }

    public void handleSyncPayload(String source, List<TableMetadata> metadata, Set<String> fullSync) {
        Address src = Utils.fromBase64(source);

        for (String table : metadata.stream().map(TableMetadata::getName).collect(Collectors.toSet())) {
            if (fullSync.contains(table)) {
                syncTableTotally(src, table);
            } else {
                syncTablePartially(src, table,
                        metadata.stream().filter(s -> s.name.equals(table)).findFirst().orElse(null));
            }
        }
    }

    private void syncTablePartially(Address src, String toSyncPartially, TableMetadata remoteTableMetadata) {
        CountingBloomFilter cbf = remoteTableMetadata.getBloomFilter();
        SyncKVTable table = syncKV.getTable(toSyncPartially);
        if (!(Arrays.equals(cbf.toByteArray(), remoteTableMetadata.bloomFilter) && remoteTableMetadata.count == table.table.size())) {
            MVMap<String, byte[]> hashes = syncKV.getTable(toSyncPartially).tableHashMetadata;
            sendDataInChunks(src, toSyncPartially, table.keys(), key -> !cbf.membershipTest(Utils.toKey(hashes.get(key))), table);
        }
    }


    private void sendDataInChunks(Address src, String name, Iterator<String> s, Predicate<String> conditionToAdd, SyncKVTable table) {
        int i = 0;
        Map<String, PayloadAndTime> payload = new HashMap<>();
        for (; s.hasNext(); ) {
            String key = s.next();
            if (conditionToAdd.test(key)) {
                i++;
                payload.put(key, new PayloadAndTime(table.get(key), table.getInsertTime(key)));
            }

            //chunk
            if (i == 200) {
                dataToSync(src, name, payload);
                payload = new HashMap<>();
                i = 0;
            }
        }

        if (payload.size() > 0) {
            dataToSync(src, name, payload);
        }
    }

    private void syncTableTotally(Address src, String name) {
        SyncKVTable table = syncKV.getTable(name);
        sendDataInChunks(src, name, table.keys(), s -> true, table);
    }

    //-----------

    void syncPayloadForLeader(Address address, Address currentAddress, List<TableMetadata> tableMetadataForSync) {
        send(address, new MethodCall("handleSyncPayloadForLeader", new Object[]{Utils.addressToBase64(currentAddress), tableMetadataForSync}, new Class[]{String.class, List.class}));
    }

    public void handleSyncPayloadForLeader(String src, List<TableMetadata> payload) {
        syncPayloads.put(Utils.fromBase64(src), payload);
    }

    //-----------

    byte[] getValue(Address src, String table, String key) {
        JChannel channel = syncKV.channel;
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());

        MethodCall call = new MethodCall("handleGetValue", new Object[]{Utils.addressToBase64(src), table, key}, new Class[]{String.class, String.class, String.class});
        byte[] res = null;
        try {
            RspList<byte[]> resp = rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.SYNC().setTimeout(50));
            res = resp.getResults().stream().filter(Objects::nonNull).findFirst().orElse(null);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling getValue", e);
        }
        return res;
    }

    public byte[] handleGetValue(String src, String table, String key) {
        Address address = Utils.fromBase64(src);
        if(address.equals(getCurrentAddress())) {
            return null;
        } else {
            return syncKV.getTable(table).get(key);
        }
    }

    //-----------


    void broadcastToEverybodyElse(MethodCall call) {
        try {
            JChannel channel = syncKV.channel;
            List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());
            rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling broadcastToEverybodyElse", e);
        }
    }

    void send(Address address, MethodCall call) {
        try {
            rpcDispatcher.callRemoteMethod(address, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling send", e);
        }
    }
}
