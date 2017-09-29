package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.SyncKV.SyncKVTable;
import ch.digitalfondue.synckv.SyncKVMessage.*;
import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.h2.mvstore.MVMap;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

class MessageReceiver extends ReceiverAdapter {

    private final Address currentAddress;
    private final SyncKV syncKV;
    private final JChannel channel;
    private final Map<Address, SyncPayloadToLeader> syncPayloads;
    private final AtomicLong lastDataSync = new AtomicLong();

    MessageReceiver(SyncKV syncKV) {
        this.currentAddress = syncKV.channel.getAddress();
        this.channel = syncKV.channel;
        this.syncKV = syncKV;
        this.syncPayloads = syncKV.syncPayloads;
    }

    @Override
    public void receive(Message msg) {
        // ignore messages sent to itself, except if they are of the type "SyncPayloadFrom" (that are internally dispatched by RequestForSyncPayloadSender)
        if (!(msg.getObject() instanceof SyncPayloadFrom) && currentAddress.equals(msg.src())) {
            return;
        }

        try {
            //
            Object payload = msg.getObject();
            if (payload instanceof SyncPayload) {
                handleSyncPayload(msg.src(), (SyncPayload) payload);
            } else if (payload instanceof DataToSync) {
                handleDataToSync((DataToSync) payload);
                lastDataSync.set(System.currentTimeMillis());
            } else if (payload instanceof RequestForSyncPayload) {
                handleRequestForSyncPayload(msg.src());
            } else if (payload instanceof SyncPayloadToLeader) {
                handleSyncPayloadForLeader(msg.src(), (SyncPayloadToLeader) payload);
            } else if (payload instanceof SyncPayloadFrom) {
                handleSyncPayloadFrom((SyncPayloadFrom) payload);
            } else if (payload instanceof PutRequest) {
                handlePutRequest((PutRequest) payload);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void handlePutRequest(PutRequest payload) {
        syncKV.getTable(payload.table).put(payload.key, payload.payload, false);
    }

    private boolean canIgnoreMessage() {
        return System.currentTimeMillis() - lastDataSync.get() <= 10 * 500;
    }

    private void handleSyncPayloadFrom(SyncPayloadFrom payload) {
        if (canIgnoreMessage()) {
            return;
        }

        payload.addressesAndTables.forEach((encodedAddress, tables) -> {
            Address target = Utils.fromBase64(encodedAddress);
            List<TableMetadata> tableMetadata = new ArrayList<>();
            Set<String> fullSync = new HashSet<>();
            tables.forEach(t -> {
                tableMetadata.add(syncKV.getTableMetadata(t.table));
                if (t.fullSync) {
                    fullSync.add(t.table);
                }
            });
            SyncKVMessage.send(channel, target, new SyncPayload(tableMetadata, fullSync));
        });
    }

    private void handleSyncPayloadForLeader(Address src, SyncPayloadToLeader payload) {
        syncPayloads.put(src, payload);
    }

    private void handleRequestForSyncPayload(Address leader) {

        if (canIgnoreMessage()) {
            return;
        }

        SyncKVMessage.send(channel, leader, new SyncKVMessage.SyncPayloadToLeader(syncKV.getTableMetadataForSync()));

    }

    private void handleDataToSync(DataToSync payload) {
        SyncKV.SyncKVTable table = syncKV.getTable(payload.name);
        payload.payload.forEach((k, v) -> {
            if (!table.present(k, v.payload) || table.isNewer(k, v.time)) {
                table.put(k, v.payload);
            }
        });
        syncKV.commit();
    }


    private void handleSyncPayload(Address src, SyncPayload s) {
        for (String table : s.getRemoteTables()) {
            if (s.fullSync.contains(table)) {
                syncTableTotally(src, table);
            } else {
                syncTablePartially(src, table, s.getMetadataFor(table));
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
        DataToSync dts = new DataToSync(name);
        for (; s.hasNext(); ) {
            String key = s.next();
            if (conditionToAdd.test(key)) {
                i++;
                dts.payload.put(key, new PayloadAndTime(table.get(key), table.getInsertTime(key)));
            }

            //chunk
            if (i == 200) {
                SyncKVMessage.send(channel, src, dts);
                dts = new DataToSync(name);
                i = 0;
            }
        }

        if (dts.payload.size() > 0) {
            SyncKVMessage.send(channel, src, dts);
        }
    }

    private void syncTableTotally(Address src, String name) {
        SyncKVTable table = syncKV.getTable(name);
        sendDataInChunks(src, name, table.keys(), s -> true, table);
    }
}
