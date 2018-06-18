package ch.digitalfondue.synckv;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SyncKVTable {

    private final SecureRandom random;
    private final RpcFacade rpcFacade;
    private final JChannel channel;
    private final MVMap<byte[], byte[]> table;

    //nanoTime and random.nextLong
    private static final int METADATA_LENGTH = 2 * Long.BYTES;
    private final MerkleTreeVariantRoot syncTree;

    public SyncKVTable(String tableName, MVStore store, SecureRandom random, RpcFacade rpcFacade, JChannel channel, MerkleTreeVariantRoot syncTree) {
        this.random = random;
        this.rpcFacade = rpcFacade;
        this.channel = channel;
        this.table = store.openMap(tableName);
        this.syncTree = syncTree;
    }

    public Set<String> keySet() {
        return table.keySet()
                .stream()
                .map(s -> new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8)) //trim away the metadata
                .collect(Collectors.toCollection(TreeSet::new)); //keep the order and remove duplicate keys
    }

    Set<String> rawKeySet() {
        return table.keySet().stream()
                .map(s -> new String(s, 0, s.length - METADATA_LENGTH, StandardCharsets.UTF_8) +
                        "_" + s[s.length - 4] + "_" + s[s.length - 3] + "_" + s[s.length - 2] + "_" + s[s.length - 1])
                .collect(Collectors.toSet());
    }

    // the key are structured as:
    // + is = concatenation
    //
    // key.bytes+nanoTime+seed
    public synchronized boolean put(String key, byte[] value) {
        long time = System.nanoTime();

        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer bf = ByteBuffer.allocate(rawKey.length + METADATA_LENGTH);
        bf.put(rawKey);
        //
        bf.putLong(time);
        bf.putLong(random.nextLong());
        //

        byte[] finalKey = bf.array();

        if (rpcFacade != null) {
            rpcFacade.putRequest(channel.getAddress(), table.getName(), finalKey, value);
        }

        addRawKV(finalKey, value);

        return true;
    }

    synchronized void addRawKV(byte[] key, byte[] value) {
        if (!table.containsKey(key)) {
            syncTree.add(key);
            table.put(key, value);
        }
    }


    public byte[] get(String key) {
        byte[][] res = get(key, true);
        return res != null ? res[1] : null;
    }


    //fetching a key, mean that we need to iterate as we may have multiple value for the same key
    //as the key are sorted, we only need to get the last one that have the same prefix and the same length (adjusted)
    byte[][] get(String key, boolean distributed) {
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
        if (distributed && res == null && rpcFacade != null) { //try to fetch the value in the cluster if it's not present locally
            byte[][] remote = rpcFacade.getValue(channel.getAddress(), table.getName(), key);

            //add value if it's missing
            if (remote != null && remote[0] != null) {
                addRawKV(remote[0], remote[1]);
            }
            //
            return remote;
        } else {
            return new byte[][]{selectedKey, res};
        }
    }

    public byte[] getRawKV(byte[] k) {
        return table.get(k);
    }
}
