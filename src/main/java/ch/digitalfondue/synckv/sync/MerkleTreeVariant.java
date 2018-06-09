package ch.digitalfondue.synckv.sync;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.ToIntFunction;

/**
 * Inspired by https://bitcoin.stackexchange.com/questions/51423/how-do-you-create-a-merkle-tree-that-lets-you-insert-and-delete-elements-without/52811#52811 .
 *
 * See https://wiki.apache.org/cassandra/AntiEntropy .
 *
 * Childs may be 2 (for binary selection) or more
 *
 */
public class MerkleTreeVariant {


    private final MerkleTreeVariant[] childs;
    private final SortedSet<ByteBuffer> content;

    public MerkleTreeVariant(int depth, int breadth, ToIntFunction<ByteBuffer> hashFunction) {

        if(depth == 0) {
            this.content = new TreeSet<>();
            this.childs = null;
        } else {
            int childDepth = depth - 1;
            this.childs = new MerkleTreeVariant[breadth];
            for(int i = 0; i < breadth; i++) {
                this.childs[i] = new MerkleTreeVariant(childDepth, breadth, hashFunction);
            }
            this.content = null;
        }
    }
}
