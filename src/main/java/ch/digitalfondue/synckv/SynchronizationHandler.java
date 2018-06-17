package ch.digitalfondue.synckv;

import org.jgroups.Address;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

class SynchronizationHandler implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(SynchronizationHandler.class.getName());


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
        /*if (syncKV.isLeader() && !running.get()) {
            try {
                running.set(true);
                processRequestForSync();
                rpcFacade.requestForSyncPayload(syncKV.getAddress());
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Error while processing the synchronization process", t);
            } finally {
                running.set(false);
            }
        }*/
    }

    /*private static class AddressCollapsibleTableMetadata {
        private final Address address;
        private final int hash;
        private final int keyCount;

        private AddressCollapsibleTableMetadata(Address address, int hash, int keyCount) {
            this.address = address;
            this.hash = hash;
            this.keyCount = keyCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hash, keyCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof AddressCollapsibleTableMetadata)) {
                return false;
            }
            AddressCollapsibleTableMetadata other = (AddressCollapsibleTableMetadata) obj;
            return hash == other.hash && keyCount == other.keyCount;
        }
    }

    private void processRequestForSync() {
        HashMap<Address, TableAndPartialTreeData[]> workingCopy = new HashMap<>(syncKV.syncPayloads);
        syncKV.syncPayloads.clear();

        //
        workingCopy.put(syncKV.getAddress(), syncKV.getTableMetadataForSync());
        //

        Map<String, Set<AddressCollapsibleTableMetadata>> tableToMetadata = new HashMap<>();
        Map<String, Set<Address>> tablePresenceToAddress = new HashMap<>();

        for (Map.Entry<Address, TableAndPartialTreeData[]> e : workingCopy.entrySet()) {
            for (TableAndPartialTreeData tm : e.getValue()) {

                if (!tableToMetadata.containsKey(tm.name)) {
                    tableToMetadata.putIfAbsent(tm.name, new HashSet<>());
                }

                if (!tablePresenceToAddress.containsKey(tm.name)) {
                    tablePresenceToAddress.put(tm.name, new HashSet<>());
                }

                tableToMetadata.get(tm.name).add(new AddressCollapsibleTableMetadata(e.getKey(), tm.hash, tm.keyCount));
                tablePresenceToAddress.get(tm.name).add(e.getKey());
            }
        }

        Map<Address, List<TableAddress>> tablesToSync = new HashMap<>();
        for (Address a : workingCopy.keySet()) {
            tablesToSync.putIfAbsent(a, new ArrayList<>());
            for (String name : tablePresenceToAddress.keySet()) {
                if (!tablePresenceToAddress.get(name).contains(a)) {
                    //missing table in the current address
                    Address toFetch = tableToMetadata.get(name).stream().findFirst().orElseThrow(IllegalStateException::new).address;
                    tablesToSync.get(a).add(new TableAddress(name, Utils.addressToBase64(toFetch)));
                } else {
                    //find the first table where the top hash or the keyCount is not equal
                    TableAndPartialTreeData first = Stream.of(workingCopy.get(a)).filter(s -> s.name.equals(name)).findFirst().orElse(null);
                    tableToMetadata.get(name).stream()
                            .filter(remote -> !remote.address.equals(a))
                            .filter(remote -> remote.hash != first.hash || remote.keyCount != first.keyCount)
                            .findFirst().ifPresent(remote -> tablesToSync.get(a).add(new TableAddress(name, Utils.addressToBase64(remote.address))));
                }
            }
        }

        tablesToSync.entrySet().stream().filter(kv -> !kv.getValue().isEmpty()).forEach(kv -> {
            this.rpcFacade.syncPayloadFrom(kv.getKey(), kv.getValue());
        });
    }*/
}
