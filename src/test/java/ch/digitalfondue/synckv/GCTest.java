package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class GCTest {

    @Test
    public void kvCleanupTest() {
        try (SyncKV kv = new SyncKV(null, null, null, null)) {
            SyncKVTable table = kv.getTable("test");
            table.put("test", "test");
            Assert.assertEquals(1, table.keySet().size());
            Assert.assertEquals(1, table.rawKeySet().size());
            Assert.assertEquals("test", table.getAsString("test"));
            table.put("test", "test2");
            Assert.assertEquals(1, table.keySet().size());
            Assert.assertEquals(2, table.rawKeySet().size());
            Assert.assertEquals("test2", table.getAsString("test"));

            List<Map.Entry<String, byte[]>> keysWithRawKey = table.getKeysWithRawKey();

            OldKVCollector oldKVCollector = new OldKVCollector(kv);

            oldKVCollector.run();



            Assert.assertEquals(1, table.keySet().size());
            Assert.assertEquals(1, table.rawKeySet().size());
        }
    }
}
