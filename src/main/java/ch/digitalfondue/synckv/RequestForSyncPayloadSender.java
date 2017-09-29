package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.JChannel;

import java.util.*;

class RequestForSyncPayloadSender implements Runnable {

    private final JChannel channel;
    private final Map<Address, List<TableMetadata>> syncPayloads;
    private final SyncKV syncKV;

    RequestForSyncPayloadSender(SyncKV syncKV) {
        this.channel = syncKV.channel;
        this.syncPayloads = syncKV.syncPayloads;
        this.syncKV = syncKV;
    }

    @Override
    public void run() {
        //only if leader
        if (channel.getView().getMembers().get(0).equals(channel.getAddress())) {
            processRequestForSync();
            this.syncKV.rpcFacade.broadcastToEverybodyElse(channel, RpcFacade.requestForSyncPayloadMethodCall(channel.getAddress()));
        }
    }

    //using only bloomFilter as a condition, we collapse all the similar one in the HashSet
    private static class AddressBloomFilter {
        private final Address address;
        private final byte[] bloomFilter;

        private AddressBloomFilter(Address address, byte[] bloomFilter) {
            this.address = address;
            this.bloomFilter = bloomFilter;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bloomFilter);
        }

        @Override
        public boolean equals(Object obj) {
            if(obj == null || ! (obj instanceof AddressBloomFilter)) {
                return false;
            }
            return Arrays.equals(bloomFilter, ((AddressBloomFilter) obj).bloomFilter);
        }
    }

    //given the currently present sync payload request, route the requests correctly between the elements of the cluster
    private void processRequestForSync() {
        Map<Address, List<TableMetadata>> workingCopy = new HashMap<>(syncPayloads);
        syncPayloads.clear();

        // add own copy
        workingCopy.put(channel.getAddress(), syncKV.getTableMetadataForSync());
        //

        //
        Map<String, Set<AddressBloomFilter>> tablePresenceCollapsed = new HashMap<>();
        Map<String, Set<Address>> tablePresence = new HashMap<>();
        for (Map.Entry<Address, List<TableMetadata>> e : workingCopy.entrySet()) {
            e.getValue().stream().forEach(tm -> {

                if (!tablePresenceCollapsed.containsKey(tm.getName())) {
                    tablePresenceCollapsed.putIfAbsent(tm.getName(), new HashSet<>());
                }

                if (!tablePresence.containsKey(tm.getName())) {
                    tablePresence.put(tm.getName(), new HashSet<>());
                }

                tablePresenceCollapsed.get(tm.getName()).add(new AddressBloomFilter(e.getKey(), tm.bloomFilter));
                tablePresence.get(tm.getName()).add(e.getKey());
            });
        }

        Map<Address, List<TableAddress>> tablesToSync = new HashMap<>();

        // for each address -> check what table is missing
        for (Address a : workingCopy.keySet()) {
            tablesToSync.putIfAbsent(a, new ArrayList<>());
            for (String name : tablePresence.keySet()) {
                if(!tablePresence.get(name).contains(a)) {
                    //missing table in the current address
                    Address toFetch = tablePresenceCollapsed.get(name).stream().findFirst().orElseThrow(IllegalStateException::new).address;
                    tablesToSync.get(a).add(new TableAddress(name, Utils.addressToBase64(toFetch), true));
                } else {
                    //handle case where table is present
                    byte[] cbf = workingCopy.get(a).stream().filter(s -> s.name.equals(name)).findFirst().orElse(null).bloomFilter;

                    //find the first table where the bloom filter is not equal
                    tablePresenceCollapsed.get(name).stream()
                            .filter(remote -> !remote.address.equals(a))
                            .filter(remote -> !Arrays.equals(remote.bloomFilter, cbf))
                            .findFirst().ifPresent(remote -> tablesToSync.get(a).add(new TableAddress(name, Utils.addressToBase64(remote.address), false))
                    );
                }
            }
        }

        tablesToSync.entrySet().stream().filter(kv -> !kv.getValue().isEmpty()).forEach(kv -> {
            this.syncKV.rpcFacade.send(kv.getKey(), RpcFacade.syncPayloadFromMethodCall(kv.getValue()));
        });

    }
}
