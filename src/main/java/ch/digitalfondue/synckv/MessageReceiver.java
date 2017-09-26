package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.SyncKVMessage.*;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

import java.util.Set;

class MessageReceiver extends ReceiverAdapter {

    private final Address currentAddress;
    private final SyncKV syncKV;

    public MessageReceiver(Address currentAddress, SyncKV syncKV) {
        this.currentAddress = currentAddress;
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
        System.err.println("both present are " + bothPresent);
    }

    private void syncTableTotally(Address src, String name) {

    }
}
