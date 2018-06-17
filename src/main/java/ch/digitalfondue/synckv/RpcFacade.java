package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.MerkleTreeVariantRoot.ExportLeaf;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RpcFacade {

    private final static Logger LOGGER = Logger.getLogger(RpcFacade.class.getName());

    private final SyncKV syncKV;
    private RpcDispatcher rpcDispatcher;

    public RpcFacade(SyncKV syncKV) {
        this.syncKV = syncKV;
    }

    public void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
        this.rpcDispatcher = rpcDispatcher;
    }

    private Address getCurrentAddress() {
        return syncKV.getChannel().getAddress();
    }

    // --- PUT ---------

    void putRequest(Address source, String table, byte[] key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall("handlePutRequest", new Object[]{Utils.addressToBase64(source), table, key, value}, new Class[]{String.class, String.class, byte[].class, byte[].class}));
    }

    public void handlePutRequest(String src, String table, byte[] key, byte[] value) {

        Address source = Utils.fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncKV.getTable(table).addRawKV(key, value);
    }
    // -----------------


    // --- GET ---------

    private static final Comparator<byte[][]> DESC_METADATA_ORDER = Comparator.<byte[][], ByteBuffer>comparing(payload -> ByteBuffer.wrap(payload[0])).reversed();

    byte[][] getValue(Address src, String table, String key) {
        JChannel channel = syncKV.getChannel();
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());

        if (everybodyElse.isEmpty()) {
            return null;
        }

        MethodCall call = new MethodCall("handleGetValue", new Object[]{Utils.addressToBase64(src), table, key}, new Class[]{String.class, String.class, String.class});
        byte[][] res = null;
        try {
            RspList<byte[][]> rsps = rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.SYNC().setTimeout(50));

            //fetch the value with the "biggest" metadata concatenate(insertion_time,seed)
            res = rsps.getResults()
                    .stream()
                    .filter(payload -> payload != null && payload[0] != null)
                    .sorted(DESC_METADATA_ORDER)
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling getValue", e);
        }
        return res;
    }

    public byte[][] handleGetValue(String src, String table, String key) {
        Address address = Utils.fromBase64(src);
        if (address.equals(getCurrentAddress())) {
            return null;
        } else {
            return syncKV.getTable(table).get(key, false);
        }
    }

    // --- SYNC --------
    // --- STEP 1 ------
    void requestForSyncPayload(Address address) {
        if (syncKV.isSyncInProgress()) {
            return;
        }

        try {
            syncKV.syncInProgress();
            broadcastToEverybodyElse(new MethodCall("handleRequestForSyncPayload", new Object[]{Utils.addressToBase64(address)}, new Class[]{String.class}));
        } finally {
            syncKV.syncInProgressDone();
        }
    }

    public void handleRequestForSyncPayload(String src) {
        if (syncKV.isSyncInProgress()) {
            return;
        }

        try {
            syncKV.syncInProgress();
            Address leader = Utils.fromBase64(src);

            if (leader.equals(getCurrentAddress())) {
                //calling himself, ignore
                return;
            }

            syncPayloadForLeader(leader, getCurrentAddress(), syncKV.getTableMetadataForSync());
        } finally {
            syncKV.syncInProgressDone();
        }
    }

    // --- STEP 2 ------
    void syncPayloadForLeader(Address address, Address currentAddress, TableAndPartialTreeData[] tableMetadataForSync) {
        try {
            syncKV.syncInProgress();
            send(address, new MethodCall("handleSyncPayloadForLeader", new Object[]{Utils.addressToBase64(currentAddress), tableMetadataForSync}, new Class[]{String.class, TableAndPartialTreeData[].class}));
        } finally {
            syncKV.syncInProgressDone();
        }
    }

    public void handleSyncPayloadForLeader(String src, TableAndPartialTreeData[] payload) {
        syncKV.syncPayloads.put(Utils.fromBase64(src), payload);
    }
    // -----------------

    // --- STEP 3 ------
    void syncPayloadFrom(Address address, List<TableAddress> remote) {
        send(address, new MethodCall("handleSyncPayloadFrom", new Object[]{remote}, new Class[]{List.class}));
    }

    public void handleSyncPayloadFrom(List<TableAddress> remote) {
        if (syncKV.isSyncInProgress()) {
            return;
        }

        try {
            syncKV.syncInProgress();

            Map<String, List<TableAddress>> addressesAndTables = new HashMap<>();
            remote.stream().forEach(ta -> {
                if (!addressesAndTables.containsKey(ta.addressEncoded)) {
                    addressesAndTables.put(ta.addressEncoded, new ArrayList<>());
                }
                addressesAndTables.get(ta.addressEncoded).add(ta);
            });

            Address currentAddress = getCurrentAddress();
            addressesAndTables.forEach((encodedAddress, tables) -> {
                Address target = Utils.fromBase64(encodedAddress);
                List<TableMetadata> tableMetadata = new ArrayList<>();
                tables.forEach(t -> {
                    MerkleTreeVariantRoot root = syncKV.getTableTree(t.table);
                    tableMetadata.add(new TableMetadata(t.table, root == null ? null : root.exportLeafStructureOnly()));
                });
                syncPayload(target, currentAddress, tableMetadata);
            });

        } finally {
            syncKV.syncInProgressDone();
        }
    }

    void syncPayload(Address addressToSend, Address address, List<TableMetadata> metadata) {
        send(addressToSend, new MethodCall("handleSyncPayload", new Object[]{Utils.addressToBase64(address), metadata}, new Class[]{String.class, List.class}));
    }
    // -----------------

    // --- STEP 4 ------
    public void handleSyncPayload(String source, List<TableMetadata> metadata) {
        if (syncKV.isSyncInProgress()) {
            return;
        }

        try {
            syncKV.syncInProgress();
            Address src = Utils.fromBase64(source);

            for (TableMetadata tableMetadata : metadata) {
                syncTable(src, tableMetadata);
            }
        } finally {
            syncKV.syncInProgressDone();
        }
    }

    void syncTable(Address src, TableMetadata tableMetadata) {
        MerkleTreeVariantRoot root = syncKV.getTableTree(tableMetadata.name);
        Set<ExportLeaf> local = new HashSet<>(Arrays.asList(root.exportLeafStructureOnly()));
        Set<ExportLeaf> remote = new HashSet<>(Arrays.asList(tableMetadata.tableMetadata));

        if (root != null) {
            sendPaths(src, tableMetadata.name, difference(local, remote));
            requirePaths(src, tableMetadata.name, difference(remote, local));
        } else {
            requireAll(src, tableMetadata.name);
        }
    }

    private void sendPaths(Address src, String name, Set<ExportLeaf> paths) {
        if (paths.isEmpty()) {
            return;
        }

        MerkleTreeVariantRoot root = syncKV.getTableTree(name);
        SyncKVTable table = syncKV.getTable(name);
        List<byte[][]> toSend = new ArrayList<>();
        for (ExportLeaf l : paths) {
            SortedSet<ByteBuffer> keysForPath = root.getKeysForPath(l.getPath());
            for (ByteBuffer bb : keysForPath) {
                byte[] key = bb.array();
                toSend.add(new byte[][]{key, table.getRawKV(key)});
            }
        }
        if (!toSend.isEmpty()) {
            send(src, new MethodCall("receiveBulkKV", new Object[]{name, toSend}, new Class[]{String.class, List.class}));
        }
    }

    private void requirePaths(Address src, String name, Set<ExportLeaf> paths) {
        if (paths.isEmpty()) {
            return;
        }

        System.err.println(getCurrentAddress() + " require paths from " + src);
    }

    private void requireAll(Address src, String name) {
        System.err.println(getCurrentAddress() + " require all " + name + " from " + src);
    }

    public static Set<ExportLeaf> difference(Set<ExportLeaf> a, Set<ExportLeaf> b) {
        Set<ExportLeaf> ca = new HashSet<>(a);
        a.removeAll(b);
        return ca;
    }

    // -----------------
    // --- STEP 5 ------
    public void receiveBulkKV(String tableName, List<byte[][]> kvs) {
        try {
            syncKV.syncInProgress();
            System.err.println(getCurrentAddress() + ": received " + kvs.size() + " kv for table " + tableName);
            SyncKVTable table = syncKV.getTable(tableName);
            for (byte[][] kv : kvs) {
                table.addRawKV(kv[0], kv[1]);
            }
        } finally {
            syncKV.syncInProgressDone();
        }
    }
    // -----------------

    void broadcastToEverybodyElse(MethodCall call) {
        try {
            JChannel channel = syncKV.getChannel();
            List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());
            if (everybodyElse.isEmpty()) {
                return;
            }
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
    // -----------------
}
