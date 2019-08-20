package ch.digitalfondue.synckv;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.jgroups.JChannel;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SyncKVTable {

    private final SecureRandom random;
    private final RpcFacade rpcFacade;
    private final JChannel channel;
    private final MVMap<byte[], byte[]> table;

    //currentTimeInMilli and nanoTime and random.nextInt
    private static final int METADATA_LENGTH = Long.BYTES + Long.BYTES + Integer.BYTES;
    private static final byte[] FLOOR_METADATA = new byte[METADATA_LENGTH]; //<- filled with -128
    private final MerkleTreeVariantRoot syncTree;
    private final AtomicBoolean disableSync;

    static {
        Arrays.fill(FLOOR_METADATA, Byte.MIN_VALUE);
    }

    SyncKVTable(String tableName, MVStore store, SecureRandom random, RpcFacade rpcFacade, JChannel channel, MerkleTreeVariantRoot syncTree, AtomicBoolean disableSync) {
        this.random = random;
        this.rpcFacade = rpcFacade;
        this.channel = channel;

        MVMap.Builder b = new MVMap.Builder<>();
        b.setKeyType(new ByteArrayDataType());
        b.setValueType(new ByteArrayDataType());

        this.table = store.openMap(tableName, b);
        this.syncTree = syncTree;
        this.disableSync = disableSync;
    }

    private static class ByteArrayDataType implements DataType {

        @Override
        public int compare(Object a, Object b) {
            return ByteBuffer.wrap((byte[]) a).compareTo(ByteBuffer.wrap((byte[]) b));
        }

        @Override
        public int getMemory(Object obj) {
            byte[] r = (byte[]) obj;
            return 24 + r.length;
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            buff.put((byte[]) obj);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public byte[] read(ByteBuffer buff) {
            return buff.array();
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }
    }

    public Set<String> keySet() {
        return table.keySet()
                .stream()
                .map(s -> new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8)) //trim away the metadata
                .collect(Collectors.toCollection(TreeSet::new)); //keep the order and remove duplicate keys
    }

    Set<String> rawKeySet() {
        return table.keySet()
                .stream()
                .map(s -> {
                    String res = new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8);
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(s, s.length - METADATA_LENGTH, s.length));
                    try {
                        return res + "_" + dis.readLong() + "_" + dis.readLong() + "_" + dis.readInt();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }).collect(Collectors.toCollection(TreeSet::new));
    }

    List<Map.Entry<String, byte[]>> getKeysWithRawKey() {
        return table.keySet().stream().map(s -> {
            String res = new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8);
            return new AbstractMap.SimpleImmutableEntry<>(res, s);
        }).collect(Collectors.toList());
    }



    public int count() {
        return keySet().size();
    }

    public Iterator<String> keys() {
        return keySet().iterator();
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

        if (rpcFacade != null && !disableSync.get()) {
            rpcFacade.putRequest(channel.getAddress(), table.getName(), finalKey, value);
        }

        addRawKV(finalKey, value);

        return true;
    }

    synchronized void deleteRawKV(byte[] key) {
        syncTree.delete(key);
        table.remove(key);
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
            syncTree.add(key);
            table.put(key, value);
        }
    }

    private boolean containsNewerKey(byte[] rawKey) {
        //
        Iterator<byte[]> n = table.keyIterator(rawKey);
        if(n.hasNext()) {
            byte[] nextKey = n.next();
            int adjustedLength = rawKey.length - METADATA_LENGTH;
            //
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
        return table.keySet().stream().map(key -> new KV(key, table.get(key))).collect(Collectors.toList());
    }

    void importRawData(List<KV> tablePayload) {
        for (KV kv : tablePayload) {
            addRawKV(kv.k, kv.v);
        }
    }

    public <T> SyncKVStructuredTable<T> toStructured(Class<T> clazz, SyncKVStructuredTable.DataConverterFrom<T> from, SyncKVStructuredTable.DataConverterTo<T> to) {
        return new SyncKVStructuredTable<>(this, from, to);
    }
}
