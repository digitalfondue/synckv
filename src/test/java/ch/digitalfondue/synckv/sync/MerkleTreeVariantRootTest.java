package ch.digitalfondue.synckv.sync;

import org.junit.Assert;
import org.junit.Test;

public class MerkleTreeVariantRootTest {


    @Test
    public void testTreeBuilding() {
        //3 ** 7 = 2187 buckets
        MerkleTreeVariantRoot r1 = new MerkleTreeVariantRoot(3, 7);
        r1.add(new byte[]{0,1});
        r1.add(new byte[]{1,0});


        //inverted insertion order
        MerkleTreeVariantRoot r2 = new MerkleTreeVariantRoot(3, 7);
        r2.add(new byte[]{1,0});
        r2.add(new byte[]{0,1});

        //resulting hash must be equal, as the tree has the exact same shape
        Assert.assertEquals(r1.getHash(), r2.getHash());
    }
}
