package ch.digitalfondue.synckv;

import org.jgroups.Address;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

class SynchronizationHandler implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(SynchronizationHandler.class.getName());


    private final SyncKV syncKV;
    private final RpcFacade rpcFacade;
    private AtomicBoolean running = new AtomicBoolean(false);
    private final Random random = new Random();

    SynchronizationHandler(SyncKV syncKV, RpcFacade rpcFacade) {
        this.syncKV = syncKV;
        this.rpcFacade = rpcFacade;
    }

    @Override
    public void run() {

        if (!running.get()) {
            try {
                running.set(true);
                List<Address> a = new ArrayList<>(syncKV.getClusterMembers());
                a.remove(syncKV.getAddress());

                if (!a.isEmpty()) {
                    Address randomAddress = a.get(Math.abs(random.nextInt()) % a.size());
                    synchronizeDB(randomAddress);
                }
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Error while processing the synchronization process", t);
            } finally {
                running.set(false);
            }
        }
    }

    private void synchronizeDB(Address address) {
        try {
            Map<String, TableAndPartialTreeData> remote = rpcFacade.getTableMetadataForSync(address).get();

            Map<String, TableAndPartialTreeData> local = syncKV.getTableMetadataForSync();

            remote.forEach((tableName, remoteMetadata) -> {
                if (local.containsKey(tableName)) {
                    TableAndPartialTreeData localMetadata = local.get(tableName);
                    if (remoteMetadata.hash != localMetadata.hash || remoteMetadata.keyCount != localMetadata.keyCount) {
                        syncTable(address, tableName, false);
                    } else {
                        System.err.println("No need to sync!");
                    }
                } else {
                    syncTable(address, tableName, true);
                }
            });
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.log(Level.WARNING, "Error while calling synchronizeDB", e);
        }
    }

    private void syncTable(Address remote, String tableName, boolean fullSync) {
        try {
            System.err.println(syncKV.getClusterMemberName() + " syncTable with " + remote + " for table " + tableName + " full sync: " + fullSync);
            if (fullSync) {
                //full sync code here
                List<byte[][]> tablePayload = rpcFacade.getFullTableData(remote, tableName).get();
                syncKV.getTable(tableName).importRawData(tablePayload);
            } else {
                //partial sync here
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.log(Level.WARNING, "Error while calling syncTable", e);
        }
    }
}
