package ch.digitalfondue.synckv.sync;

import org.junit.Test;

public class MerkleTreeVariantRootTest {


    @Test
    public void testTreeBuilding() {
        //3 ** 7 = 2187 buckets
        MerkleTreeVariantRoot r = new MerkleTreeVariantRoot(3, 7, MurmurHash::hash);

        r.add(new byte[]{0,1});
    }
}
