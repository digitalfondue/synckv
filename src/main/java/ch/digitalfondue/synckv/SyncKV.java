package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SyncKV implements Closeable {

    final MVStore store;
    final ConcurrentHashMap<String, CountingBloomFilter> bloomFilters = new ConcurrentHashMap<>();
    final JChannel channel;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    final Map<Address, List<TableMetadataWithHashedBloomFilter>> syncPayloads = new ConcurrentHashMap<>();
    final RpcFacade rpcFacade;

    public SyncKV() throws Exception {
        this(null, "SyncKV");
    }

    static final Predicate<String> IS_VALID_PUBLIC_TABLE_NAME = name -> !name.contains("__");

    public SyncKV(String fileName, String channelName) throws Exception {
        store = MVStore.open(fileName);
        store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).forEach(name -> {
            bloomFilters.putIfAbsent(name, Utils.bloomFilterInstance());

            CountingBloomFilter cbf = bloomFilters.get(name);

            MVMap<String, byte[]> hashes = store.openMap(name + "__metadata_hash");
            //rebuild bloom filter state from data
            for (byte[] hash : hashes.values()) {
                cbf.add(Utils.toKey(hash));
            }
        });

        channel = new JChannel();
        channel.connect(channelName);

        this.rpcFacade = new RpcFacade(this);
        RpcDispatcher rpcDispatcher = new RpcDispatcher(channel, rpcFacade);
        this.rpcFacade.setRpcDispatcher(rpcDispatcher);

        scheduledExecutor = new ScheduledThreadPoolExecutor(1);

        scheduledExecutor.scheduleAtFixedRate(new RequestForSyncPayloadSender(this), 0, 10, TimeUnit.SECONDS);
    }

    public boolean isLeader() {
        return channel.getView().getMembers().get(0).equals(channel.getAddress());
    }

    public String getClusterMemberName() {
        return channel.getAddressAsString();
    }

    public List<String> getClusterMembersName() {
        return channel.view().getMembers().stream().map(Address::toString).collect(Collectors.toList());
    }

    public Set<String> getTables() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).collect(Collectors.toSet());
    }

    List<TableMetadataWithHashedBloomFilter> getTableMetadataForSync() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME)
                .sorted()
                .map(this::getTableMetadata)
                .map(tableMetadata -> new TableMetadataWithHashedBloomFilter(tableMetadata.name, tableMetadata.count, digest(tableMetadata.bloomFilter)))
                .collect(Collectors.toList());
    }

    TableMetadata getTableMetadata(String name) {
        if(store.getMapNames().contains(name)) {
            return new TableMetadata(name, store.openMap(name).size(), bloomFilters.get(name).toByteArray());
        } else {
            return new TableMetadata(name, 0, null);
        }
    }

    private static byte[] digest(byte[] input) {

        if (input == null) {
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }



    public long commit() {
        return store.commit();
    }

    @Override
    public void close() {
        store.close();
        channel.close();
        scheduledExecutor.shutdown();
    }

    public synchronized SyncKVTable getTable(String tableName) {
        Objects.requireNonNull(tableName);
        if (!IS_VALID_PUBLIC_TABLE_NAME.test(tableName)) {
            throw new IllegalArgumentException(String.format("Table name '%s' cannot contain '__' sequence", tableName));
        }

        bloomFilters.putIfAbsent(tableName, Utils.bloomFilterInstance());

        return new SyncKVTable(tableName, this);
    }

    public static class SyncKVTable {
        final MVMap<String, byte[]> table;
        final MVMap<String, byte[]> tableHashMetadata;
        final MVMap<String, Long> tableLatestInsertMetadata;
        final CountingBloomFilter countingBloomFilter;
        final SyncKV syncKV;


        private SyncKVTable(String tableName, SyncKV syncKV) {

            this.table = syncKV.store.openMap(tableName);
            this.tableHashMetadata = syncKV.store.openMap(tableName + "__metadata_hash");
            this.tableLatestInsertMetadata = syncKV.store.openMap(tableName +"__metadata_insert");
            this.countingBloomFilter = syncKV.bloomFilters.get(tableName);
            this.syncKV = syncKV;
        }

        public Iterator<String> keys() {
            return table.keyIterator(table.firstKey());
        }

        public long count() {
            return table.sizeAsLong();
        }

        static int hashFor(String key, byte[] value) {
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            byte[] kv = Utils.concatenate(k, value);
            return Utils.hash(kv);
        }

        boolean present(String key, byte[] value) {
            byte[] res = tableHashMetadata.get(key);
            return res == null ? false : Utils.toKey(hashFor(key, value)).equals(Utils.toKey(res));
        }

        public synchronized byte[] put(String key, byte[] value) {
            return put(key, value, true);
        }

        public synchronized String put(String key, String value) {
            byte[] res = put(key, value.getBytes(StandardCharsets.UTF_8));
            return res == null ? null : new String(res, StandardCharsets.UTF_8);
        }

        synchronized byte[] put(String key, byte[] value, boolean broadcast) {
            int hash = hashFor(key, value);

            byte[] oldRes = table.put(key, value);
            if (oldRes != null) {
                Key oldKey = Utils.toKey(tableHashMetadata.get(key));
                countingBloomFilter.delete(oldKey);
            }
            Key newKey = Utils.toKey(hash);
            tableHashMetadata.put(key, newKey.getBytes());
            tableLatestInsertMetadata.put(key, System.nanoTime());
            countingBloomFilter.add(newKey);

            if (broadcast) {
                syncKV.rpcFacade.putRequest(syncKV.channel.getAddress(), table.getName(), key, value);
            }

            return oldRes;
        }

        public byte[] get(String key) {
            return get(key, false);
        }

        public String getAsString(String key) {
            byte[] res = get(key);
            return res == null ? null : new String(res, StandardCharsets.UTF_8);
        }

        byte[] get(String key, boolean localOnly) {
            byte[] res = table.get(key);
            if(localOnly != true && res == null) { //try to fetch the value in the cluster if it's not present locally
                res = syncKV.rpcFacade.getValue(syncKV.channel.getAddress(), table.getName(), key);
            }
            return res;
        }

        long getInsertTime(String key) {
            return tableLatestInsertMetadata.get(key);
        }

        boolean isNewer(String k, long time) {
            return time > getInsertTime(k);
        }
    }
}
