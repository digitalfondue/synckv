package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.JChannel;

import java.util.*;

class RequestForSyncPayloadSender implements Runnable {

    private final JChannel channel;
    private final Map<Address, SyncKVMessage.SyncPayloadToLeader> syncPayloads;
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
            SyncKVMessage.broadcast(channel, new SyncKVMessage.RequestForSyncPayload());
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
        Map<Address, SyncKVMessage.SyncPayloadToLeader> workingCopy = new HashMap<>(syncPayloads);
        syncPayloads.clear();

        // add own copy
        workingCopy.put(channel.getAddress(), new SyncKVMessage.SyncPayloadToLeader(syncKV.getTableMetadataForSync()));
        //

        //
        Map<String, Set<AddressBloomFilter>> tablePresenceCollapsed = new HashMap<>();
        Map<String, Set<Address>> tablePresence = new HashMap<>();
        for (Map.Entry<Address, SyncKVMessage.SyncPayloadToLeader> e : workingCopy.entrySet()) {
            e.getValue().metadata.stream().forEach(tm -> {

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

        Map<Address, List<SyncKVMessage.TableAddress>> tablesToSync = new HashMap<>();

        // for each address -> check what table is missing
        for (Address a : workingCopy.keySet()) {
            tablesToSync.putIfAbsent(a, new ArrayList<>());
            for (String name : tablePresence.keySet()) {
                if(!tablePresence.get(name).contains(a)) {
                    //missing table in the current address
                    Address toFetch = tablePresenceCollapsed.get(name).stream().findFirst().orElseThrow(IllegalStateException::new).address;
                    tablesToSync.get(a).add(new SyncKVMessage.TableAddress(name, toFetch, true));
                } else {
                    //handle case where table is present
                    byte[] cbf = workingCopy.get(a).getMetadataFor(name).bloomFilter;

                    //find the first table where the bloom filter is not equal
                    tablePresenceCollapsed.get(name).stream()
                            .filter(remote -> !remote.address.equals(a))
                            .filter(remote -> !Arrays.equals(remote.bloomFilter, cbf))
                            .findFirst().ifPresent(remote -> tablesToSync.get(a).add(new SyncKVMessage.TableAddress(name, remote.address, false))
                    );
                }
            }
        }

        tablesToSync.entrySet().stream().filter(kv -> !kv.getValue().isEmpty()).forEach(kv -> {
            SyncKVMessage.send(channel, kv.getKey(), new SyncKVMessage.SyncPayloadFrom(kv.getValue()));
        });

    }
}
