package ch.digitalfondue.synckv;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
class MerkleTreeVariantRoot implements NodeWithUpdateHashAndChildPosition {

    private final Node[] children;
    private volatile int hash;
    private final byte depth;
    private final AtomicInteger keyCount = new AtomicInteger();

    MerkleTreeVariantRoot(byte depth, byte breadth) {
        this.children = new Node[breadth];
        this.depth = depth;
    }

    List<Export> exportStructureOnly() {
        List<Export> export = new ArrayList<>();
        export.add(new ExportRoot(depth, (byte) children.length, hash, keyCount.get()));
        for (Node n : children) {
            if (n != null) {
                n.export(export);
            }
        }
        return export;
    }

    static abstract class Export implements Serializable {
        final byte depth;

        private Export(byte depth) {
            this.depth = depth;
        }
    }

    static class ExportRoot extends Export {

        private final int hash;
        private final byte breadth;
        private final int keyCount;

        private ExportRoot(byte depth, byte breadth, int hash, int keyCount) {
            super(depth);
            this.breadth = breadth;
            this.hash = hash;
            this.keyCount = keyCount;
        }

        @Override
        public String toString() {
            return String.format("ExportRoot{depth: %d, hash: %d, breadth: %d, keyCount: %d}", depth, hash, breadth, keyCount);
        }
    }

    static class ExportNode extends Export {
        private final int hash;
        private final byte position;

        private ExportNode(byte depth, int hash, byte position) {
            super(depth);
            this.hash = hash;
            this.position = position;
        }

        @Override
        public String toString() {
            return String.format("ExportNode{depth: %d, hash: %d, position %d}", depth, hash, position);
        }
    }

    static class ExportLeaf extends Export {
        private final int hash;
        private final byte position;
        private final int keyCount;

        private ExportLeaf(byte depth, int hash, byte position, int keyCount) {
            super(depth);
            this.hash = hash;
            this.position = position;
            this.keyCount = keyCount;
        }

        @Override
        public String toString() {
            return String.format("ExportLeaf{depth: %d, hash: %d, position: %d, keyCount: %d}", depth, hash, position, keyCount);
        }
    }

    synchronized void add(byte[] value) {

        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = Math.abs(hashWrappedValue % children.length);

        if (children[bucket] == null) {
            children[bucket] = new Node((byte) (depth - 1), (byte) children.length, this);
        }

        children[bucket].add(wrapped, hashWrappedValue - bucket);

        hash = computeHashFor(children);
        keyCount.incrementAndGet();
    }

    @Override
    public void updateHash() {
    }

    @Override
    public byte position(NodeWithUpdateHashAndChildPosition child) {
        return position(children, child);
    }

    int getHash() {
        return hash;
    }

    private static byte position(Node[] children, NodeWithUpdateHashAndChildPosition node) {
        for(byte i = 0; i < children.length; i++) {
            if(children[i] == node) {
                return i;
            }
        }
        return -1;
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


    private static class Node implements NodeWithUpdateHashAndChildPosition {
        private Node[] children;
        private SortedSet<ByteBuffer> content;
        private volatile int hash;
        private final byte depth;
        private final byte breadth;
        private NodeWithUpdateHashAndChildPosition parent;

        Node(byte depth, byte breadth, NodeWithUpdateHashAndChildPosition parent) {
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
                children[bucket] = new Node((byte) (depth - 1), breadth, this);
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

        @Override
        public void updateHash() {
            hash = computeHashFor(children);
            if (parent != null) {
                parent.updateHash();
            }
        }

        @Override
        public byte position(NodeWithUpdateHashAndChildPosition child) {
            return MerkleTreeVariantRoot.position(children, child);
        }

        private static Export getLast(List<Export> l) {
            return l.size() > 0 ? l.get(l.size() - 1) : null;
        }

        void export(List<Export> export) {
            if (depth == 0) {
                export.add(new ExportLeaf(depth, hash, parent.position(this), content == null ? 0 : content.size()));
            } else {
                export.add(new ExportNode(depth, hash, parent.position(this)));
            }
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    Node n = children[i];
                    if (n != null) {
                        n.export(export);
                    }
                }
            }
        }
    }
}
