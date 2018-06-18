package ch.digitalfondue.synckv;

import org.jgroups.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

class SynchronizationHandler implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(SynchronizationHandler.class.getName());


    private final SyncKV syncKV;
    private final RpcFacade rpcFacade;
    private final Random random = new Random();

    SynchronizationHandler(SyncKV syncKV, RpcFacade rpcFacade) {
        this.syncKV = syncKV;
        this.rpcFacade = rpcFacade;
    }

    @Override
    public void run() {

        if (syncKV.disableSync.get()) {
            return;
        }

        //only one is run in a single synckv instance
        try {
            LOGGER.log(Level.FINE, "Running sync");
            List<Address> a = new ArrayList<>(syncKV.getClusterMembers());
            a.remove(syncKV.getAddress());

            if (!a.isEmpty()) {
                Address randomAddress = a.get(Math.abs(random.nextInt()) % a.size());
                synchronizeDB(randomAddress);
            }
            LOGGER.log(Level.FINE, "End running sync");
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Error while processing the synchronization process", t);
        }
    }

    private void synchronizeDB(Address address) {
        try {
            Map<String, TableStats> remote = rpcFacade.getTableMetadataForSync(address).join();

            Map<String, TableStats> local = syncKV.getTableMetadataForSync();

            remote.forEach((tableName, remoteMetadata) -> {
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
            if (fullSync) {
                //full sync code here
                List<byte[][]> tablePayload = rpcFacade.getFullTableData(remote, tableName).join();
                syncKV.getTable(tableName).importRawData(tablePayload);
            } else {
                //partial sync here
                List<MerkleTreeVariantRoot.ExportLeaf> exportLeaves = syncKV.getTableTree(tableName).exportLeafStructureOnly();
                List<byte[][]> tablePayload = rpcFacade.getPartialTableData(remote, tableName, exportLeaves).join();
                syncKV.getTable(tableName).importRawData(tablePayload);
            }
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Error while calling syncTable", e);
        }
    }
}
