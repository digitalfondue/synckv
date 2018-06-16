package ch.digitalfondue.synckv;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Inspired by https://bitcoin.stackexchange.com/questions/51423/how-do-you-create-a-merkle-tree-that-lets-you-insert-and-delete-elements-without/52811#52811 .
 * <p>
 * See https://wiki.apache.org/cassandra/AntiEntropy .
 * <p>
 * Variation:
 * - number of children and depth is configurable
 * - the bucket selection is done using (hash % number of childs)
 * - the nodes are created lazily
 */
class MerkleTreeVariantRoot {

    private final Node[] children;
    private volatile int hash;
    private final int depth;
    private final AtomicInteger keyCount = new AtomicInteger();

    MerkleTreeVariantRoot(int depth, int breadth) {
        this.children = new Node[breadth];
        this.depth = depth;
    }

    synchronized void add(byte[] value) {

        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = Math.abs(hashWrappedValue % children.length);

        if (children[bucket] == null) {
            children[bucket] = new Node(depth - 1, children.length, null);
        }

        children[bucket].add(wrapped, hashWrappedValue - bucket);

        hash = computeHashFor(children);
        keyCount.incrementAndGet();
    }

    int getHash() {
        return hash;
    }

    private static int computeHashFor(Node[] children) {
        ByteBuffer hashes = ByteBuffer.allocate(children.length * Integer.BYTES);
        for (Node c : children) {
            hashes.putInt(c != null ? c.hash : 0);
        }
        return MurmurHash.hash(hashes);
    }

    int getKeyCount() {
        return keyCount.get();
    }


    private static class Node {
        private Node[] children;
        private SortedSet<ByteBuffer> content;
        private volatile int hash;
        private final int depth;
        private final int breadth;
        private Node parent;

        Node(int depth, int breadth, Node parent) {
            this.depth = depth;
            this.breadth = breadth;
            this.parent = parent;
        }

        void add(ByteBuffer wrapped, int resultingHash) {
            if (depth == 0) {
                insertValue(wrapped);
            } else {
                selectBucket(wrapped, resultingHash);
            }
        }

        private void selectBucket(ByteBuffer wrapped, int resultingHash) {
            if (children == null) {
                this.children = new Node[breadth];
            }

            int bucket = Math.abs(resultingHash % children.length);
            // lazy node creation too
            if (children[bucket] == null) {
                children[bucket] = new Node(depth - 1, breadth, this);
            }
            //
            children[bucket].add(wrapped, resultingHash - bucket);
        }

        private void insertValue(ByteBuffer wrapped) {
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
        }

        private void updateHash() {
            hash = computeHashFor(children);
            if (parent != null) {
                parent.updateHash();
            }
        }
    }
}
