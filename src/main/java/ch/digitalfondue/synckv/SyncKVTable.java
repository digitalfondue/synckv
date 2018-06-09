package ch.digitalfondue.synckv;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SyncKVTable {

    private final long seed;
    private final RpcFacade rpcFacade;
    private final JChannel channel;
    private final MVMap<byte[], byte[]> table;

    private static final int METADATA_LENGTH = 2 * Long.BYTES;

    public SyncKVTable(String tableName, MVStore store, long seed, RpcFacade rpcFacade, JChannel channel) {
        this.seed = seed;
        this.rpcFacade = rpcFacade;
        this.channel = channel;
        this.table = store.openMap(tableName);
    }


    // the key are structured as:
    // + is = concatenation
    //
    // key.bytes+nanoTime+seed
    public boolean put(String key, byte[] value) {
        return put(key, value, true);
    }

    public Set<String> keySet() {
        return table.keySet()
                .stream()
                .map(s -> new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8)) //trim away the metadata
                .collect(Collectors.toCollection(TreeSet::new)); //keep the order
    }

    synchronized boolean put(String key, byte[] value, boolean broadcast) {
        long time = System.nanoTime();

        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer bf = ByteBuffer.allocate(rawKey.length + METADATA_LENGTH);
        bf.put(rawKey);
        //
        bf.putLong(time);
        bf.putLong(seed);
        //

        if (broadcast) {
            rpcFacade.putRequest(channel.getAddress(), table.getName(), key, value);
        }

        table.put(bf.array(), value);
        return true;
    }


    public byte[] get(String key) {
        return get(key, false);
    }


    //fetching a key, mean that we need to iterate as we may have multiple value for the same key
    //as the key are sorted, we only need to get the last one that have the same prefix and the same length (adjusted)
    byte[] get(String key, boolean localOnly) {
        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        int adjustedKeyLength = rawKey.length + METADATA_LENGTH;
        ByteBuffer keyBb = ByteBuffer.wrap(rawKey);

        Iterator<byte[]> it = table.keyIterator(rawKey);

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
        if (!localOnly && res == null) { //try to fetch the value in the cluster if it's not present locally
            res = rpcFacade.getValue(channel.getAddress(), table.getName(), key);
        }
        return res;
    }
}
