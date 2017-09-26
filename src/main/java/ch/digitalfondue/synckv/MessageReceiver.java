package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.SyncKVMessage.*;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

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
        if (currentAddress.equals(msg.src())) {
            return;
        }
        //
        Object payload = msg.getObject();
        if (payload instanceof SyncPayload) {
            handleSyncPayload(msg.src(), (SyncPayload) payload);
        } else if (payload instanceof DataToSync) {
            handleDataToSync((DataToSync) payload);
        }
    }

    private void handleDataToSync(DataToSync payload) {
        System.err.println(String.format("syncing table %s adding %d items", payload.name, payload.payload.size()));
        SyncKV.SyncKVTable table = syncKV.getTable(payload.name);
        payload.payload.forEach((k, v) -> {
            table.put(k, v);
        });
        syncKV.commit();
    }


    private void handleSyncPayload(Address src, SyncPayload s) {

        System.err.println(s.metadata);
        // where missing, we send everything
        Set<String> remoteMissing = syncKV.getTables();
        remoteMissing.removeAll(s.getRemoteTables());
        System.err.println("missing is " + remoteMissing);
        for (String toSyncTotally : remoteMissing) {
            syncTableTotally(src, toSyncTotally);
        }


        // where both present, we use the remote bloom filter to send only the one that are _not_ present -> obviously before we check that count and bloom filter are not equals!
        Set<String> bothPresent = syncKV.getTables();
        bothPresent.retainAll(s.getRemoteTables());

        for (String toSyncPartially : bothPresent) {
            syncTablePartially(src, toSyncPartially);
        }

        System.err.println("both present are " + bothPresent);
    }

    private void syncTablePartially(Address src, String toSyncPartially) {
    }

    private void syncTableTotally(Address src, String name) {
        SyncKV.SyncKVTable table = syncKV.getTable(name);

        int i = 0;
        DataToSync dts = new DataToSync(name, new HashMap<>());
        for (Iterator<String> s = table.keys(); s.hasNext(); ) {
            String key = s.next();
            i++;
            dts.payload.put(key, table.get(key));

            if (i == 200) {
                try {
                    channel.send(src, dts);
                    dts = new DataToSync(name, new HashMap<>());
                } catch (Exception e) {
                    // ignore
                }

                i = 0;
            }
        }
        try {
            if (dts.payload.size() > 0) {
                channel.send(src, dts);
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
