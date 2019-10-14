package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TreeSyncTest {

    @Test
    public void testTreeBuilding() {
        //3 ** 7 = 2187 buckets
        TreeSync r1 = new TreeSync((byte) 3, (byte) 7);

        r1.add(new byte[]{0, 1});
        Assert.assertEquals(1, r1.getCount());
        r1.add(new byte[]{1, 0});
        Assert.assertEquals(2, r1.getCount());
        r1.add(new byte[]{125, 31, 1, 1});
        Assert.assertEquals(3, r1.getCount());

        List<TreeSync.ExportLeaf> e = r1.exportLeafStructureOnly();
        Assert.assertEquals(2, e.size());
        Assert.assertEquals(3, r1.getCount());
        Assert.assertEquals(3,e.get(0).keyCount + e.get(1).keyCount);
    }
}
