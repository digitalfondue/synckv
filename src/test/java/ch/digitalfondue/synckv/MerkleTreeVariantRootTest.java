package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MerkleTreeVariantRootTest {


    @Test
    public void testTreeBuilding() {
        //3 ** 7 = 2187 buckets
        MerkleTreeVariantRoot r1 = new MerkleTreeVariantRoot((byte) 3, (byte) 7);
        r1.add(new byte[]{0, 1});
        r1.add(new byte[]{1, 0});
        r1.add(new byte[]{125, 31, 1, 1});


        //inverted insertion order
        MerkleTreeVariantRoot r2 = new MerkleTreeVariantRoot((byte) 3, (byte) 7);
        r2.add(new byte[]{1, 0});
        r2.add(new byte[]{125, 31, 1, 1});
        r2.add(new byte[]{0, 1});

        //resulting hash must be equal, as the tree has the exact same shape
        Assert.assertEquals(r1.getHash(), r2.getHash());

        //r1.exportStructureOnly().forEach(System.err::println);
    }

    private static int serializationLength(Object a) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(a);
        return baos.size();
    }
}
