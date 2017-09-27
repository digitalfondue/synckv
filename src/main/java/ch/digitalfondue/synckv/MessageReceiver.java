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

    public MessageReceiver(SyncKV syncKV) {
        this.currentAddress = syncKV.channel.getAddress();
        this.channel = syncKV.channel;
        this.syncKV = syncKV;
        this.syncPayloads = syncKV.syncPayloads;
    }

    @Override
    public void receive(Message msg) {
        // ignore messages sent to itself
        if (currentAddress.equals(msg.src())) {
            return;
        }

        try {
            //
            Object payload = msg.getObject();

            System.err.println(String.format("%s: received message %s", currentAddress.toString(), payload));
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
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void handleSyncPayloadFrom(SyncPayloadFrom payload) {
        if(System.currentTimeMillis() - lastDataSync.get() <= 10*1000) {
            System.err.println("Ignore handleSyncPayloadFrom as we had a sync 10s before");
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

        if(System.currentTimeMillis() - lastDataSync.get() > 10*1000) {
            SyncKVMessage.send(channel, leader, new SyncKVMessage.SyncPayloadToLeader(syncKV.getTableMetadataForSync()));
        } else {
            System.err.println("Ignore handleRequestForSyncPayload, as we had a sync 10s before");
        }
    }

    private void handleDataToSync(DataToSync payload) {
        System.err.println(String.format("%s: syncing table %s adding %d items", currentAddress.toString(), payload.name, payload.payload.size()));
        SyncKV.SyncKVTable table = syncKV.getTable(payload.name);
        payload.payload.forEach((k, v) -> {
            if (!table.present(k, v.payload) || table.isNewer(k, v.time)) {
                System.err.println(String.format("%s: adding key %s", currentAddress.toString(), k));
                table.put(k, v.payload);
            } else {
                System.err.println(String.format("%s: key %s is already present", currentAddress.toString(), k));
            }
        });
        syncKV.commit();
    }


    private void handleSyncPayload(Address src, SyncPayload s) {
        System.err.println(currentAddress + ": Handle sync payload " + s);
        for (String table : s.getRemoteTables()) {
            if (s.fullSync.contains(table)) {
                System.err.println("Sync totally " + table);
                syncTableTotally(src, table);
            } else {
                System.err.println("Sync partially " + table);
                syncTablePartially(src, table, s.getMetadataFor(table));
            }
        }
    }

    private void syncTablePartially(Address src, String toSyncPartially, TableMetadata remoteTableMetadata) {
        CountingBloomFilter cbf = remoteTableMetadata.getBloomFilter();
        SyncKVTable table = syncKV.getTable(toSyncPartially);

        if (Arrays.equals(cbf.toByteArray(), remoteTableMetadata.bloomFilter) &&
                remoteTableMetadata.count == table.table.size()) {
            System.err.println(String.format("%s: Table %s has the same content and bloom filter", currentAddress.toString(), toSyncPartially));
            //same content
        } else {
            System.err.println(String.format("%s: will sync partially %s", currentAddress.toString(), toSyncPartially));
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
        System.err.println(String.format("%s: will sync totally %s to %s", currentAddress.toString(), name, src.toString()));
        SyncKVTable table = syncKV.getTable(name);
        sendDataInChunks(src, name, table.keys(), s -> true, table);
    }
}
