package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;
import ch.digitalfondue.synckv.bloom.MurmurHash;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.Base64;

class Utils {

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


    static String addressToBase64(JChannel channel) {
        return addressToBase64(channel.getAddress());
    }

    static String addressToBase64(Address address) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream();
        try {
            Util.writeAddress(address, out);
            return Base64.getEncoder().encodeToString(out.getByteBuffer().array());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Address fromBase64(String encoded) {
        try {
            return Util.readAddress(new ByteArrayDataInputStream(Base64.getDecoder().decode(encoded)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
