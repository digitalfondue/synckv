package ch.digitalfondue.synckv.sync;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.ToIntFunction;

/**
 * Inspired by https://bitcoin.stackexchange.com/questions/51423/how-do-you-create-a-merkle-tree-that-lets-you-insert-and-delete-elements-without/52811#52811 .
 * <p>
 * See https://wiki.apache.org/cassandra/AntiEntropy .
 * <p>
 * Childs may be 2 (for binary selection) or more
 */
public class MerkleTreeVariantRoot {


    private final Node[] childs;
    private final ToIntFunction<ByteBuffer> hashFunction;
    private int hash;

    public MerkleTreeVariantRoot(int depth, int breadth, ToIntFunction<ByteBuffer> hashFunction) {
        this.hashFunction = hashFunction;
        int childDepth = depth - 1;
        this.childs = new Node[breadth];
        for (int i = 0; i < breadth; i++) {
            this.childs[i] = new Node(childDepth, breadth, hashFunction);
        }
    }

    public synchronized void add(byte[] value) {
        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hash = hashFunction.applyAsInt(wrapped);
        int bucket = hash % childs.length;
        childs[bucket].add(wrapped, hash - bucket);
    }


    private static class Node {
        private Node[] childs;
        private Set<ByteBuffer> content;
        private int hash;
        private final int depth;
        private final int breadth;
        private final ToIntFunction<ByteBuffer> hashFunction;


        Node(int depth, int breadth, ToIntFunction<ByteBuffer> hashFunction) {
            this.depth = depth;
            this.breadth = breadth;
            this.hashFunction = hashFunction;
        }

        private void lazyBuild() {
            if (depth == 0) {
                this.childs = null;
                this.content = new TreeSet<>();
            } else {
                this.content = null;
                this.childs = new Node[breadth];
            }
        }

        void add(ByteBuffer wrapped, int resultingHash) {
            //
            if (childs == null && content == null) {
                lazyBuild();
            }
            //

            //TODO: calc hash and propagate to parent!

            if (content != null) {
                content.add(wrapped);
            } else {
                int bucket = resultingHash % childs.length;
                if(childs[bucket] == null) {
                    childs[bucket] = new Node(depth - 1, breadth, hashFunction);
                }
                childs[bucket].add(wrapped, resultingHash - bucket);
            }
        }
    }
}
