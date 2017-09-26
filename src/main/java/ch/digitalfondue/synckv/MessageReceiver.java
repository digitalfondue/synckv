package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.SyncKVMessage.*;
import ch.digitalfondue.synckv.SyncKV.SyncKVTable;
import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.h2.mvstore.MVMap;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

class MessageReceiver extends ReceiverAdapter {

    private final Address currentAddress;
    private final SyncKV syncKV;
    private final JChannel channel;

    public MessageReceiver(JChannel channel, SyncKV syncKV) {
        this.currentAddress = channel.getAddress();
        this.channel = channel;
        this.syncKV = syncKV;
    }

    @Override
    public void receive(Message msg) {
        // ignore messages sent to itself
        if (currentAddress.equals(msg.src())) {
            return;
        }

        //
        Object payload = msg.getObject();

        System.err.println(String.format("%s: received message %s", currentAddress.toString(), payload));
        if (payload instanceof SyncPayload) {
            handleSyncPayload(msg.src(), (SyncPayload) payload);
        } else if (payload instanceof DataToSync) {
            handleDataToSync((DataToSync) payload);
        }
    }

    private void handleDataToSync(DataToSync payload) {
        System.err.println(String.format("%s: syncing table %s adding %d items", currentAddress.toString(), payload.name, payload.payload.size()));
        SyncKV.SyncKVTable table = syncKV.getTable(payload.name);
        payload.payload.forEach((k, v) -> {
            if(!table.present(k,v)) {
                System.err.println(String.format("%s: adding key %s", currentAddress.toString(), k));
                table.put(k, v);
            } else {
                System.err.println(String.format("%s: key %s is already present", currentAddress.toString(), k));
            }
        });
        syncKV.commit();
    }


    private void handleSyncPayload(Address src, SyncPayload s) {

        System.err.println(s.metadata);

        // where missing, we send everything
        Set<String> remoteMissing = syncKV.getTables();
        remoteMissing.removeAll(s.getRemoteTables());

        for (String toSyncTotally : remoteMissing) {
            syncTableTotally(src, toSyncTotally);
        }


        // where both present, we use the remote bloom filter to send only the one that are _not_ present -> obviously before we check that count and bloom filter are not equals!
        Set<String> bothPresent = syncKV.getTables();
        bothPresent.retainAll(s.getRemoteTables());

        for (String toSyncPartially : bothPresent) {
            syncTablePartially(src, toSyncPartially, s.getMetadataFor(toSyncPartially));
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
                dts.payload.put(key, table.get(key));
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
