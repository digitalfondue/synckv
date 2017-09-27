package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.JChannel;

import java.util.*;

public class RequestForSyncPayloadSender implements Runnable {

    private final JChannel channel;
    private final Map<Address, SyncKVMessage.SyncPayloadToLeader> syncPayloads;
    private final SyncKV syncKV;

    public RequestForSyncPayloadSender(SyncKV syncKV) {
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

                    // for each table -> pick a random one that everybody else must sync bidirectionally to it? (if count > 1 obviously!)
                    //FIXME
                }
            }
        }

        System.err.println(channel.getAddress()+": Tables to sync is " + tablesToSync);
        tablesToSync.entrySet().stream().filter(kv -> !kv.getValue().isEmpty()).forEach(kv -> {
            System.err.println(channel.getAddress()+": Sending SyncPayloadFrom to " + kv.getKey());
            SyncKVMessage.send(channel, kv.getKey(), new SyncKVMessage.SyncPayloadFrom(kv.getValue()));
        });
    }
}
