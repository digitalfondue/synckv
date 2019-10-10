package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class GCTest {

    @Test
    public void kvCleanupTest() {

        try (SyncKV kv = new SyncKV(null, null, null, null)) {

            Iterator<String> keys;

            SyncKVTable table = kv.getTable("test");
            Assert.assertEquals(0, table.count());

            keys = table.keys();
            Assert.assertFalse(keys.hasNext());

            kv.disableCompacting(true);
            table.put("test", "test");
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(1, table.rawKeySet().size());
            Assert.assertEquals("test", table.getAsString("test"));

            keys = table.keys();
            Assert.assertEquals("test", keys.next());
            Assert.assertFalse(keys.hasNext());

            table.put("test", "test2");
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(2, table.rawKeySet().size());
            Assert.assertEquals("test2", table.getAsString("test"));

            keys = table.keys();
            Assert.assertEquals("test", keys.next());
            Assert.assertFalse(keys.hasNext());

            table.put("test", "test3");
            Assert.assertEquals(1, table.count());
            Assert.assertEquals(3, table.rawKeySet().size());
            Assert.assertEquals("test3", table.getAsString("test"));

            keys = table.keys();
            Assert.assertEquals("test", keys.next());
            Assert.assertFalse(keys.hasNext());

            table.put("test1", "next");

            Assert.assertEquals(2, table.count());
            Assert.assertEquals(4, table.rawKeySet().size());
            Assert.assertEquals("test3", table.getAsString("test"));

            keys = table.keys();
            Assert.assertEquals("test", keys.next());
            Assert.assertEquals("test1", keys.next());
            Assert.assertFalse(keys.hasNext());;


            OldKVCollector oldKVCollector = new OldKVCollector(kv);

            kv.disableCompacting(false);
            oldKVCollector.run();

            Assert.assertEquals("test3", table.getAsString("test"));
            Assert.assertEquals("next", table.getAsString("test1"));
            Assert.assertEquals(2, table.count());
            Assert.assertEquals(2, table.rawKeySet().size());
        }
    }
}
