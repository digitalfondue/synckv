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

            MVMap<String, byte[]> hashes = store.openMap(name + "__metadata_hash");
            //rebuild bloom filter state from data
            for (byte[] hash : hashes.values()) {
                cbf.add(Utils.toKey(hash));
            }
        });

        channel = new JChannel();
        channel.connect(channelName);
        channel.setReceiver(new MessageReceiver(channel, this));

        scheduledExecutor = new ScheduledThreadPoolExecutor(1);

        scheduledExecutor.scheduleAtFixedRate(new SynchronizerMessageSender(store, channel, bloomFilters), 0, 10, TimeUnit.SECONDS);
    }

    public Set<String> getTables() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).collect(Collectors.toSet());
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
                bloomFilters.get(tableName));
    }

    public static class SyncKVTable {
        final MVMap<String, byte[]> table;
        final MVMap<String, byte[]> tableHashMetadata;
        final CountingBloomFilter countingBloomFilter;


        private SyncKVTable(MVMap<String, byte[]> table,
                            MVMap<String, byte[]> tableHashMetadata,
                            CountingBloomFilter countingBloomFilter) {
            this.tableHashMetadata = tableHashMetadata;
            this.table = table;
            this.countingBloomFilter = countingBloomFilter;
        }

        public Iterator<String> keys() {
            return table.keyIterator(table.firstKey());
        }

        static int hashFor(String key, byte[] value) {
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            byte[] kv = Utils.concatenate(k, value);
            return Utils.hash(kv);
        }

        public boolean present(String key, byte[] value) {
            byte[] res = tableHashMetadata.get(key);
            return res == null ? false : Utils.toKey(hashFor(key, value)).equals(Utils.toKey(res));
        }

        public synchronized byte[] put(String key, byte[] value) {
            int hash = hashFor(key, value);

            byte[] oldRes = table.put(key, value);
            if (oldRes != null) {
                Key oldKey = Utils.toKey(tableHashMetadata.get(key));
                countingBloomFilter.delete(oldKey);
            }
            Key newKey = Utils.toKey(hash);
            tableHashMetadata.put(key, newKey.getBytes());
            countingBloomFilter.add(newKey);
            return oldRes;
        }

        public byte[] get(String key) {
            return table.get(key);
        }
    }
}
