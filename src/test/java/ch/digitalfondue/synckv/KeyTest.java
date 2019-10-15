package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class KeyTest {

    @Test
    public void keyTest() {

        try (SyncKV kv = new SyncKV(null, null, null, null)) {
            SyncKVTable test = kv.getTable("test");

            test.put("key", "value");
            test.put("key", "value2");

            List<byte[]> keysWithRawKey = new ArrayList<>(test.rawKeyWithOldValuesSet());
            Assert.assertEquals(2, keysWithRawKey.size());

            //checking raw keys

            //first key is less than second key
            Assert.assertTrue(SyncKVTable.compareKey(keysWithRawKey.get(0), keysWithRawKey.get(1)) < 0);

            //first key is equal to first key (doh)
            Assert.assertTrue(SyncKVTable.compareKey(keysWithRawKey.get(0), keysWithRawKey.get(0)) == 0);

            //second key is equal to second key (doh)
            Assert.assertTrue(SyncKVTable.compareKey(keysWithRawKey.get(1), keysWithRawKey.get(1)) == 0);

            //second key is bigger than first key
            Assert.assertTrue(SyncKVTable.compareKey(keysWithRawKey.get(1), keysWithRawKey.get(0)) > 0);

            // get the latest key
            Comparator<byte[]> c = (kv1, kv2) -> SyncKVTable.compareKey(kv1, kv2);
            Assert.assertEquals(0, ByteBuffer.wrap(keysWithRawKey.stream().sorted(c.reversed()).collect(Collectors.toList()).get(0)).compareTo(ByteBuffer.wrap(keysWithRawKey.get(1))));
            //
        }
    }
}
