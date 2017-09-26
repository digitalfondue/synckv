package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;


import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SyncKV {

    private final MVStore store;
    private final ConcurrentHashMap<String, CountingBloomFilter> bloomFilters = new ConcurrentHashMap<>();
    private final JChannel channel;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    public SyncKV() throws Exception {
        this(null, "SyncKV");
    }

    static final Predicate<String> IS_VALID_PUBLIC_TABLE_NAME = name -> !name.contains("__");

    public SyncKV(String fileName, String channelName) throws Exception {
        store = MVStore.open(fileName);
        store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).forEach(name -> {
            bloomFilters.putIfAbsent(name, Utils.bloomFilterInstance());

            CountingBloomFilter cbf = bloomFilters.get(name);

            MVMap<String, Integer> hashes = store.openMap(name + "__metadata_hash");
            //rebuild bloom filter state from data
            for (Integer hash : hashes.values()) {
                cbf.add(new Key(intToByteArray(hash)));
            }
        });

        channel = new JChannel();
        channel.connect(channelName);
        channel.setReceiver(new MessageReceiver(channel, this));

        scheduledExecutor = new ScheduledThreadPoolExecutor(1);

        scheduledExecutor.scheduleAtFixedRate(new SynchronizerMessageSender(store, channel, bloomFilters), 0, 10, TimeUnit.SECONDS);
    }

    public boolean hasTableWithName(String name) {
        return store.getMapNames().contains(name);
    }

    public Set<String> getTables() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).collect(Collectors.toSet());
    }

    private static final byte[] intToByteArray(int value) {
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

    public long commit() {
        return store.commit();
    }

    public void close() {
        store.close();
        channel.close();
        scheduledExecutor.shutdown();
    }

    public void rollback() {
        store.rollback();
    }

    public void rollback(long version) {
        store.rollbackTo(version);
    }

    public synchronized SyncKVTable getTable(String tableName) {
        Objects.requireNonNull(tableName);
        if (!IS_VALID_PUBLIC_TABLE_NAME.test(tableName)) {
            throw new IllegalArgumentException(String.format("Table name '%s' cannot contain '__' sequence", tableName));
        }

        bloomFilters.putIfAbsent(tableName, Utils.bloomFilterInstance());

        return new SyncKVTable(store.openMap(tableName),
                store.openMap(tableName + "__metadata_hash"),
                store.openMap(tableName + "__metadata_time"),
                bloomFilters.get(tableName));
    }

    public static class SyncKVTable {
        private final MVMap<String, byte[]> table;
        private final MVMap<String, Integer> tableHashMetadata;
        private final MVMap<String, Long> tableTimeMetadata;
        private final CountingBloomFilter countingBloomFilter;


        private SyncKVTable(MVMap<String, byte[]> table,
                            MVMap<String, Integer> tableHashMetadata,
                            MVMap<String, Long> tableTimeMetadata,
                            CountingBloomFilter countingBloomFilter) {
            this.tableHashMetadata = tableHashMetadata;
            this.tableTimeMetadata = tableTimeMetadata;
            this.table = table;
            this.countingBloomFilter = countingBloomFilter;
        }

        public Iterator<String> keys() {
            return table.keyIterator(table.firstKey());
        }

        public synchronized byte[] put(String key, byte[] value) {
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            byte[] kv = Utils.concatenate(k, value);
            int hash = Utils.hash(kv);

            byte[] oldRes = table.put(key, value);
            if (oldRes != null) {
                Key oldKey = new Key(intToByteArray(tableHashMetadata.get(key)));
                countingBloomFilter.delete(oldKey);
            }

            tableHashMetadata.put(key, hash);
            tableTimeMetadata.put(key, System.nanoTime());
            countingBloomFilter.add(new Key(intToByteArray(hash)));
            return oldRes;
        }

        public byte[] get(String key) {
            return table.get(key);
        }
    }
}
