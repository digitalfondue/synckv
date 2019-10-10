package ch.digitalfondue.synckv;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.jgroups.JChannel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SyncKVTable {

    private final static Logger LOGGER = Logger.getLogger(SyncKVTable.class.getName());

    private final String tableName;
    private final SecureRandom random;
    private final RpcFacade rpcFacade;
    private final JChannel channel;
    private final MVMap<byte[], byte[]> table;
    private final MVStore store;
    private final AtomicReference<TableStats> tableStats = new AtomicReference<>();

    //currentTimeInMilli and nanoTime and random.nextInt
    static final int METADATA_LENGTH = Long.BYTES + Long.BYTES + Integer.BYTES;
    private final AtomicBoolean disableSync;
    private static final byte[] FLOOR_METADATA = new byte[METADATA_LENGTH]; //<- filled with -128

    static {
        Arrays.fill(FLOOR_METADATA, Byte.MIN_VALUE);
    }

    SyncKVTable(String tableName, MVStore store, SecureRandom random, RpcFacade rpcFacade, JChannel channel, AtomicBoolean disableSync) {
        this.tableName = tableName;
        this.random = random;
        this.rpcFacade = rpcFacade;
        this.channel = channel;

        MVMap.Builder b = new MVMap.Builder<>();
        b.setKeyType(new KeyByteArrayDataType());
        b.setValueType(new ValueByteArrayDataType());
        this.table = store.openMap(tableName, b);
        this.store = store;
        this.disableSync = disableSync;
    }

    static boolean sameKeyIgnoringMetadata(byte[] a, byte[] b) {
        if (a == null && b == null) {
            return true;
        }

        if ((a == null && b != null) || (a != null && b == null)) {
            return false;
        }

        if (a.length != b.length) {
            return false;
        }

        ByteBuffer keyA = ByteBuffer.wrap(a, 0, a.length - METADATA_LENGTH);
        ByteBuffer keyB = ByteBuffer.wrap(b, 0, b.length - METADATA_LENGTH);

        return keyA.equals(keyB);
    }

    static int compareKey(byte[] ba, byte[] bb) {
        ByteBuffer keyA = ByteBuffer.wrap(ba, 0, ba.length - METADATA_LENGTH);
        ByteBuffer keyB = ByteBuffer.wrap(bb, 0, bb.length - METADATA_LENGTH);

        int comparison = keyA.compareTo(keyB);

        if (comparison != 0) {
            return comparison;
        }


        ByteBuffer metadataA = ByteBuffer.wrap(ba, ba.length - METADATA_LENGTH, METADATA_LENGTH);
        ByteBuffer metadataB = ByteBuffer.wrap(bb, bb.length - METADATA_LENGTH, METADATA_LENGTH);

        long timeInMilliA = metadataA.getLong();
        long timeInMilliB = metadataB.getLong();

        comparison = Long.compare(timeInMilliA, timeInMilliB);

        if (comparison != 0) {
            return comparison;
        }

        long nanoTimeA = metadataA.getLong();
        long nanoTimeB = metadataB.getLong();

        comparison = Long.compare(nanoTimeA, nanoTimeB);

        if (comparison != 0) {
            return comparison;
        }

        int rndA = metadataA.getInt();
        int rndB = metadataB.getInt();
        comparison = Integer.compare(rndA, rndB);
        return comparison;
    }

    private static class KeyByteArrayDataType extends ValueByteArrayDataType {

        @Override
        public int compare(Object a, Object b) {
            byte[] ba = (byte[]) a;
            byte[] bb = (byte[]) b;

            return compareKey(ba, bb);
        }
    }

    private static class ValueByteArrayDataType implements DataType {

        @Override
        public int compare(Object a, Object b) {
            byte[] ba = (byte[]) a;
            byte[] bb = (byte[]) b;
            return ByteBuffer.wrap(ba).compareTo(ByteBuffer.wrap(bb));
        }

        @Override
        public int getMemory(Object obj) {
            byte[] r = (byte[]) obj;
            return 24 + r.length;
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            byte[] r = (byte[]) obj;
            buff.putInt(r.length);
            buff.put((byte[]) obj);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public Object read(ByteBuffer buff) {
            int toRead = buff.getInt();
            byte[] r = new byte[toRead];
            buff.get(r);
            return r;
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }
    }

    Set<byte[]> rawKeySet() {
        return table.keySet();
    }

    public int count() {
        int count = 0;
        Iterator<String> keys = keys();
        while(keys.hasNext()) {
            keys.next();
            count++;
        }
        return count;
    }

    private static String rawKeyToString(byte[] s) {
        return new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8);
    }

    public Iterator<String> keys() {
        PushbackIterator<byte[]> bi = new PushbackIterator<>(table.keyIterator(null));

        return new Iterator<String>() {

            @Override
            public boolean hasNext() {
                return bi.hasNext();
            }

            @Override
            public String next() {
                byte[] key = bi.next();
                if (bi.hasNext()) {
                    byte[] nextKey;
                    do {
                        nextKey = bi.next();
                        if (!sameKeyIgnoringMetadata(key, nextKey)) {
                            bi.pushback(nextKey);
                            break;
                        }
                        key = nextKey;
                    } while(bi.hasNext());
                }
                return rawKeyToString(key);
            }
        };
    }

    // the key are structured as:
    // + is = concatenation
    //
    // key.bytes+currentTime+nanoTime+seed
    public synchronized boolean put(String key, byte[] value) {
        long currentTime = System.currentTimeMillis();
        long nanoTime = System.nanoTime();

        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer bf = ByteBuffer.allocate(rawKey.length + METADATA_LENGTH);
        bf.put(rawKey);
        //
        bf.putLong(currentTime);
        bf.putLong(nanoTime);
        bf.putInt(random.nextInt());
        //

        byte[] finalKey = bf.array();

        addRawKV(finalKey, value);
        store.tryCommit();

        if (rpcFacade != null && !disableSync.get()) {
            rpcFacade.putRequest(channel.getAddress(), table.getName(), finalKey, value);
        }

        return true;
    }

    synchronized void deleteRawKV(byte[] key) {
        tableStats.set(null);
        table.remove(key);
        store.tryCommit();
    }

    public boolean put(String key, String value) {
        return put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public String getAsString(String key) {
        byte[] res = get(key);
        return res == null ? null : new String(res, StandardCharsets.UTF_8);
    }

    synchronized void addRawKV(byte[] key, byte[] value) {
        if (!table.containsKey(key) && !containsNewerKey(key)) {
            tableStats.set(null);
            table.put(key, value);
        }
    }

    private boolean containsNewerKey(byte[] rawKey) {
        //
        Iterator<byte[]> n = table.keyIterator(rawKey);
        if(n.hasNext()) {
            byte[] nextKey = n.next();
            int adjustedLength = rawKey.length - METADATA_LENGTH;
            return nextKey.length == rawKey.length && ByteBuffer.wrap(rawKey, 0, adjustedLength).equals(ByteBuffer.wrap(nextKey, 0, adjustedLength));
        }
        return false;
    }


    public byte[] get(String key) {
        KV res = get(key, true);
        return res != null ? res.v : null;
    }


    //fetching a key, mean that we need to iterate as we may have multiple value for the same key
    //as the key are sorted, we only need to get the last one that have the same prefix and the same length (adjusted)
    KV get(String key, boolean distributed) {
        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        int adjustedKeyLength = rawKey.length + METADATA_LENGTH;
        ByteBuffer keyBb = ByteBuffer.wrap(rawKey);

        //start key
        ByteBuffer startKey = ByteBuffer.allocate(adjustedKeyLength);
        startKey.put(rawKey);
        startKey.put(FLOOR_METADATA);

        Iterator<byte[]> it = table.keyIterator(startKey.array());

        byte[] selectedKey = null;

        while (it.hasNext()) {
            byte[] candidateKey = it.next();

            if (candidateKey.length == adjustedKeyLength && ByteBuffer.wrap(candidateKey, 0, rawKey.length).compareTo(keyBb) == 0) {
                selectedKey = candidateKey;
            } else {
                break;
            }
        }

        byte[] res = selectedKey != null ? table.get(selectedKey) : null;

        //
        if (distributed && res == null && rpcFacade != null && !disableSync.get()) { //try to fetch the value in the cluster if it's not present locally
            KV remote = rpcFacade.getValue(channel.getAddress(), table.getName(), key);

            //add value if it's missing
            if (remote != null && remote.k != null) {
                addRawKV(remote.k, remote.v);
                store.tryCommit();
            }
            //
            return remote;
        } else {
            return new KV(selectedKey, res);
        }
    }

    byte[] getRawKV(byte[] k) {
        return table.get(k);
    }

    List<KV> exportRawData() {
        List<KV> res = new ArrayList<>();
        for (byte[] key : table.keySet()) {
            res.add(new KV(key, table.get(key)));
        }
        return res;
    }

    void importRawData(List<KV> tablePayload) {
        for (KV kv : tablePayload) {
            addRawKV(kv.k, kv.v);
        }
        store.tryCommit();
    }

    public <T> SyncKVStructuredTable<T> toStructured(Class<T> clazz, SyncKVStructuredTable.DataConverterFrom<T> from, SyncKVStructuredTable.DataConverterTo<T> to) {
        return new SyncKVStructuredTable<>(this, from, to);
    }

    TableStats getTableStats() {
        TableStats t = tableStats.get();
        if (t != null) {
            LOGGER.log(Level.FINE, () -> "for table stats of " + tableName + " hitting cache");
            return t;
        }

        LOGGER.log(Level.FINE, () -> "for table stats of " + tableName + " must be recomputed!");

        Set<byte[]> l = table.keySet();
        int hash = 0;
        int count = 0;
        for (byte[] k : l) {
            count++;
            hash = MurmurHash.hash(k, hash);
        }

        t = new TableStats(count, hash);
        tableStats.set(t);
        return t;
    }
}
