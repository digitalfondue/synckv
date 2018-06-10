package ch.digitalfondue.synckv.sync;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Inspired by https://bitcoin.stackexchange.com/questions/51423/how-do-you-create-a-merkle-tree-that-lets-you-insert-and-delete-elements-without/52811#52811 .
 * <p>
 * See https://wiki.apache.org/cassandra/AntiEntropy .
 * <p>
 * Variation:
 * - number of childs and depth is configurable
 * - the bucket selection is done using (hash % number of childs)
 * - the nodes are created lazily
 */
public class MerkleTreeVariantRoot {

    private final Node[] childs;
    private int hash;

    public MerkleTreeVariantRoot(int depth, int breadth) {
        int childDepth = depth - 1;
        this.childs = new Node[breadth];
        for (int i = 0; i < breadth; i++) {
            this.childs[i] = new Node(childDepth, breadth, null);
        }
    }

    public synchronized void add(byte[] value) {
        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = hashWrappedValue % childs.length;
        childs[bucket].add(wrapped, hashWrappedValue - bucket);

        hash = computeHashFor(childs);
    }

    public int getHash() {
        return hash;
    }

    private static int computeHashFor(Node[] childs) {
        ByteBuffer hashes = ByteBuffer.allocate(childs.length * Integer.BYTES);
        for (Node c : childs) {
            hashes.putInt(c != null ? c.hash : 0);
        }
        return MurmurHash.hash(hashes);
    }


    private static class Node {
        private Node[] childs;
        private SortedSet<ByteBuffer> content;
        private int hash;
        private final int depth;
        private final int breadth;
        private Node parent;

        Node(int depth, int breadth, Node parent) {
            this.depth = depth;
            this.breadth = breadth;
            this.parent = parent;
        }

        void add(ByteBuffer wrapped, int resultingHash) {

            //
            if (depth == 0) {

                if (this.content == null) {
                    this.content = new TreeSet<>();
                }

                content.add(wrapped);

                //compute hash of content
                ByteBuffer hashes = ByteBuffer.allocate(content.size() * Integer.BYTES);
                for (ByteBuffer bf : content) {
                    hashes.putInt(MurmurHash.hash(bf));
                }
                hash = MurmurHash.hash(hashes);
                if (parent != null) {
                    parent.updateHash();
                }
                //
            } else {

                if (childs == null) {
                    this.childs = new Node[breadth];
                }

                int bucket = resultingHash % childs.length;
                // lazy node creation too
                if (childs[bucket] == null) {
                    childs[bucket] = new Node(depth - 1, breadth, this);
                }
                //
                childs[bucket].add(wrapped, resultingHash - bucket);
            }
        }

        private void updateHash() {
            hash = computeHashFor(childs);
            if (parent != null) {
                parent.updateHash();
            }
        }
    }
}
