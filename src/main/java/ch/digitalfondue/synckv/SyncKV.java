package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jgroups.Address;
import org.jgroups.JChannel;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
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
    final Map<Address, SyncKVMessage.SyncPayloadToLeader> syncPayloads = new ConcurrentHashMap<>();

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



        channel.setReceiver(new MessageReceiver(this));

        scheduledExecutor = new ScheduledThreadPoolExecutor(1);

        scheduledExecutor.scheduleAtFixedRate(new RequestForSyncPayloadSender(this), 0, 10, TimeUnit.SECONDS);
    }

    public Set<String> getTables() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME).collect(Collectors.toSet());
    }

    List<SyncKVMessage.TableMetadata> getTableMetadataForSync() {
        return store.getMapNames().stream().filter(IS_VALID_PUBLIC_TABLE_NAME)
                .sorted()
                .map(this::getTableMetadata)
                .collect(Collectors.toList());
    }

    SyncKVMessage.TableMetadata getTableMetadata(String name) {
        if(store.getMapNames().contains(name)) {
            return new SyncKVMessage.TableMetadata(name, store.openMap(name).size(), bloomFilters.get(name).toByteArray());
        } else {
            return new SyncKVMessage.TableMetadata(name, 0, null);
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

        return new SyncKVTable(store.openMap(tableName),
                store.openMap(tableName + "__metadata_hash"),
                store.openMap(tableName +"__metadata_insert"),
                bloomFilters.get(tableName),
                channel);
    }

    public static class SyncKVTable {
        final MVMap<String, byte[]> table;
        final MVMap<String, byte[]> tableHashMetadata;
        final MVMap<String, Long> tableLatestInsertMetadata;
        final CountingBloomFilter countingBloomFilter;
        final JChannel channel;


        private SyncKVTable(MVMap<String, byte[]> table,
                            MVMap<String, byte[]> tableHashMetadata,
                            MVMap<String, Long> tableLatestInsertMetadata,
                            CountingBloomFilter countingBloomFilter,
                            JChannel channel) {
            this.tableHashMetadata = tableHashMetadata;
            this.table = table;
            this.tableLatestInsertMetadata = tableLatestInsertMetadata;
            this.countingBloomFilter = countingBloomFilter;
            this.channel = channel;
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
            return put(key, value, true);
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
                SyncKVMessage.broadcast(channel, new SyncKVMessage.PutRequest(table.getName(), key, value));
            }

            return oldRes;
        }

        public byte[] get(String key) {
            return table.get(key);
        }

        public long getInsertTime(String key) {
            return tableLatestInsertMetadata.get(key);
        }

        public boolean isNewer(String k, long time) {
            return time > getInsertTime(k);
        }
    }
}
