package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;
import ch.digitalfondue.synckv.bloom.MurmurHash;
import org.jgroups.JChannel;

public class Utils {

    static byte[] concatenate(byte[] a, byte[] b) {
        int aLen = a.length;
        int bLen = b.length;
        byte[] c = new byte[aLen + bLen];

        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

    static CountingBloomFilter bloomFilterInstance() {
        return CountingBloomFilter.instance(60000);
    }

    static int hash(byte[] payload) {
        return MurmurHash.hash(payload);
    }

    static Key toKey(int val) {
        return new Key(intToByteArray(val));
    }

    static Key toKey(byte[] val) {
        return new Key(val);
    }

    private static final byte[] intToByteArray(int value) {
        return new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
    }

}
