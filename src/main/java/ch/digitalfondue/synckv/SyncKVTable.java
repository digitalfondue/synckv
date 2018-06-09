package ch.digitalfondue.synckv;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class SyncKVTable {

    private final long seed;
    private final MVMap<byte[], byte[]> table;

    public SyncKVTable(String tableName, MVStore store, long seed) {
        this.seed = seed;
        this.table = store.openMap(tableName);
    }


    // the key are structured as:
    // + is = concatenation
    //
    // key.bytes+nanoTime+seed
    public boolean put(String key, byte[] value) {

        long time = System.nanoTime();

        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        ByteBuffer bf = ByteBuffer.allocate(rawKey.length+2*Long.BYTES);
        bf.put(rawKey);
        //
        bf.putLong(time);
        bf.putLong(seed);
        //

        table.put(bf.array(), value);

        return true;
    }


    //fetching a key, mean that we need to iterate as we may have multiple value for the same key
    //as the key are sorted, we only need to get the last one that have the same prefix and the same length (adjusted)
    public byte[] get(String key) {

        byte[] rawKey = key.getBytes(StandardCharsets.UTF_8);

        int adjustedKeyLength = rawKey.length+2*Long.BYTES;
        ByteBuffer keyBb = ByteBuffer.wrap(rawKey);

        Iterator<byte[]> it = table.keyIterator(rawKey);

        byte[] selectedKey = null;

        while(it.hasNext()) {
            byte[] candidateKey = it.next();

            if(candidateKey.length == adjustedKeyLength && ByteBuffer.wrap(candidateKey, 0, rawKey.length).compareTo(keyBb) == 0) {
                selectedKey = candidateKey;
            } else {
                break;
            }
        }

        if(selectedKey != null) {
            return table.get(selectedKey);
        } else {
            return null;
        }
    }
}
