package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.MurmurHash;

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
}
