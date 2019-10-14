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

        TreeSync r2 = new TreeSync((byte) 3, (byte) 7);
        r2.add(new byte[]{125, 31, 1, 1});
        Assert.assertEquals(1, r2.getCount());
        List<TreeSync.ExportLeaf> e2 = r2.exportLeafStructureOnly();
        Assert.assertEquals(1, e2.size());
        //

        // sync e2 with e1
        r1.removeMatchingLeafs(e2); // we remove all the leafs in common
        //
        Assert.assertTrue(r1.isInExistingBucket(new byte[]{0, 1})); //<- key is in a existing bucket, we need to sync from r1 to r2
        Assert.assertTrue(r1.isInExistingBucket(new byte[]{1, 0})); //<- key is in a existing bucket, we need to sync from r1 to r2
        Assert.assertFalse(r1.isInExistingBucket(new byte[]{125, 31, 1, 1})); //<- key is not in a existing bucket, we don't need to sync it


    }
}
