package ch.digitalfondue.synckv;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

class SynchronizationHandler implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(SynchronizationHandler.class.getName());


    private final SyncKV syncKV;
    private final RpcFacade rpcFacade;
    private final Random random = new Random();
    private AtomicReference<Address> lastUsedAddress = new AtomicReference<>();

    SynchronizationHandler(SyncKV syncKV, RpcFacade rpcFacade) {
        this.syncKV = syncKV;
        this.rpcFacade = rpcFacade;
    }

    @Override
    public void run() {

        if (syncKV.isSyncDisabled()) {
            return;
        }

        //only one is run in a single synckv instance
        try {
            LOGGER.log(Level.FINE, "Running sync");
            List<Address> a = new ArrayList<>(syncKV.getClusterMembers());
            a.remove(syncKV.getAddress());

            // try to rotate more by removing
            Address lastUsedAddr = lastUsedAddress.get();
            if (a.size() > 1 && lastUsedAddr != null) {
                a.remove(lastUsedAddr);
            }
            //

            if (!a.isEmpty()) {
                Address randomAddress = a.get(Math.abs(random.nextInt()) % a.size());
                lastUsedAddress.set(randomAddress);
                synchronizeDB(randomAddress);
            }
            LOGGER.log(Level.FINE, "End running sync");
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Error while processing the synchronization process", t);
        }
    }

    /**
     * Synchronization is done in only one direction. The local instance will fetch the missing data against the one
     * randomly selected.
     *
     * @param address
     */
    private void synchronizeDB(Address address) {
        try {
            Map<String, TableStats> remote = rpcFacade.getTableMetadataForSync(address).join();
            Map<String, TableStats> local = syncKV.getTableMetadataForSync();

            remote.forEach((tableName, remoteMetadata) -> {

                SyncStatus syncStatus = syncKV.syncStatus.get(tableName);

                //if more than 60 seconds has elapsed from the latest refresh we consider the sync done
                if (syncStatus != null && ((System.currentTimeMillis() - syncStatus.time) > 60*1000)) {
                    syncKV.syncStatus.remove(tableName);
                }
                if (syncStatus != null && syncStatus.status) {
                    LOGGER.log(Level.INFO, ()-> "Will skip sync table '" + tableName + "', as it's still syncing");
                    return; //skip this table
                }


                if (local.containsKey(tableName)) {
                    TableStats localMetadata = local.get(tableName);
                    if (remoteMetadata.hash != localMetadata.hash || remoteMetadata.keyCount != localMetadata.keyCount) {
                        syncTable(address, tableName, false);
                    } else {
                        LOGGER.fine(() -> String.format("%s: No need to sync with remote: %s", syncKV.getClusterMemberName(), address)); //TODO better logger msg
                    }
                } else {
                    syncTable(address, tableName, true);
                }
            });
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Error while calling synchronizeDB", e);
        }
    }

    private void syncTable(Address remote, String tableName, boolean fullSync) {
        try {
            LOGGER.fine(() -> String.format("%s: Need to sync table: %s with remote: %s", syncKV.getClusterMemberName(), tableName, remote)); //TODO better logger msg
            syncKV.syncStatus.put(tableName, new SyncStatus(true, System.currentTimeMillis()));
            if (fullSync) {
                //full sync code here
                LOGGER.log(Level.FINE, () -> "Full sync for table " + tableName);
                rpcFacade.getFullTableData(remote, tableName, syncKV.getAddress());
            } else {
                //partial sync here
                List<TreeSync.ExportLeaf> exportLeaves = syncKV.getTable(tableName).getTreeSync().exportLeafStructureOnly();
                LOGGER.log(Level.FINE, () -> "Partial sync for table " + tableName);
                rpcFacade.getPartialTableData(remote, tableName, exportLeaves, syncKV.getAddress());
            }
        } catch (Throwable e) {
            syncKV.syncStatus.remove(tableName);
            LOGGER.log(Level.WARNING, "Error while calling syncTable", e);
        }
    }
}
