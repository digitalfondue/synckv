package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

public class GCTest {

    @Test
    public void kvCleanupTest() {

        try (SyncKV kv = new SyncKV(null, null, null, null)) {
            SyncKVTable table = kv.getTable("test");
            kv.disableCompacting(true);
            table.put("test", "test");
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(1, table.rawKeySet().size());
            Assert.assertEquals("test", table.getAsString("test"));
            table.put("test", "test2");
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(2, table.rawKeySet().size());
            Assert.assertEquals("test2", table.getAsString("test"));

            OldKVCollector oldKVCollector = new OldKVCollector(kv);

            kv.disableCompacting(false);
            oldKVCollector.run();

            Assert.assertEquals("test2", table.getAsString("test"));
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(1, table.rawKeySet().size());
        }
    }
}
