package ch.digitalfondue.synckv;

import org.jgroups.Address;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class SynchronizationHandler implements Runnable {

    private final SyncKV syncKV;
    private final RpcFacade rpcFacade;
    private AtomicBoolean running = new AtomicBoolean(false);

    SynchronizationHandler(SyncKV syncKV, RpcFacade rpcFacade) {
        this.syncKV = syncKV;
        this.rpcFacade = rpcFacade;
    }

    @Override
    public void run() {
        //TODO: additional condition: "currently in sync process"
        if (syncKV.isLeader() && !running.get()) {
            try {
                running.set(true);
                processRequestForSync();
                rpcFacade.requestForSyncPayload(syncKV.getAddress());
            } finally {
                running.set(false);
            }
        }
    }

    private void processRequestForSync() {
        HashMap<Address, TableAndPartialTreeData[]> workingCopy = new HashMap<>(syncKV.syncPayloads);
        syncKV.syncPayloads.clear();

        //
        workingCopy.put(syncKV.getAddress(), syncKV.getTableMetadataForSync());
        //

        Map<String, Set<TableAndPartialTreeData>> tableToMetadata = new HashMap<>();
        Map<String, Set<Address>> tablePresenceToAddress = new HashMap<>();

        for (Map.Entry<Address, TableAndPartialTreeData[]> e : workingCopy.entrySet()) {
            for (TableAndPartialTreeData tm : e.getValue()) {

                if (!tableToMetadata.containsKey(tm.name)) {
                    tableToMetadata.putIfAbsent(tm.name, new HashSet<>());
                }

                if (!tablePresenceToAddress.containsKey(tm.name)) {
                    tablePresenceToAddress.put(tm.name, new HashSet<>());
                }

                tableToMetadata.get(tm.name).add(tm);
                tablePresenceToAddress.get(tm.name).add(e.getKey());
            }
        }
    }
}
