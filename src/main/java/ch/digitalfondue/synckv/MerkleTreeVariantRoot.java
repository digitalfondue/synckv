package ch.digitalfondue.synckv;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
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
        hash = computeHashFor(children);
    }

    List<ExportLeaf> exportLeafStructureOnly() {

        List<ExportLeaf> export = new ArrayList<>();

        for (Node n : children) {
            if (n != null) {
                n.export(export);
            }
        }
        return export;
    }

    SortedSet<ByteBuffer> getKeysForPath(byte[] path) {
        Node node = children[path[0]];
        for (int i = 1; i < path.length; i++) {
            node = node.children[path[i]];
        }
        return new TreeSet<>(node.content);
    }

    static class ExportLeaf implements Serializable {
        private final int hash;
        private final int keyCount;
        private final byte[] path;

        private ExportLeaf(int hash, int keyCount, ByteArrayOutputStream baos) {
            this.hash = hash;
            this.keyCount = keyCount;
            this.path = baos.toByteArray();
        }

        @Override
        public String toString() {
            return String.format("ExportLeaf{hash: %d, keyCount: %d, path:%s}", hash, keyCount, format(path));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof ExportLeaf)) {
                return false;
            }

            ExportLeaf other = (ExportLeaf) obj;
            return hash == other.hash && keyCount == other.keyCount && Arrays.equals(path, other.path);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new int[]{Integer.hashCode(hash), Integer.hashCode(keyCount), Arrays.hashCode(path)});
        }

        byte[] getPath() {
            return path;
        }
    }

    private static String format(byte[] ar) {
        StringBuilder sb = new StringBuilder();
        for (byte b : ar) {
            sb.append(b).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    synchronized void add(byte[] value) {

        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = Math.abs(hashWrappedValue % children.length);

        if (children[bucket] == null) {
            children[bucket] = new Node((byte) (depth - 1), (byte) children.length, this);
        }

        boolean res = children[bucket].add(wrapped, hashWrappedValue - bucket);

        if (res) {
            hash = computeHashFor(children);
            keyCount.incrementAndGet();
        }
    }


    synchronized boolean delete(byte[] value) {
        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = Math.abs(hashWrappedValue % children.length);
        if (children[bucket] == null) {
            return false;
        }
        boolean res = children[bucket].delete(wrapped, hashWrappedValue - bucket);
        if (res) {
            hash = computeHashFor(children);
            keyCount.decrementAndGet();
        }
        return res;
    }

    @Override
    public void updateHash() {
    }

    @Override
    public byte position(NodeWithUpdateHashAndChildPosition child) {
        return position(children, child);
    }

    @Override
    public void path(ByteArrayOutputStream sb) {
    }

    int getHash() {
        return hash;
    }

    private static byte position(Node[] children, NodeWithUpdateHashAndChildPosition node) {
        if (children == null) {
            return -1;
        }
        for (byte i = 0; i < children.length; i++) {
            if (children[i] == node) {
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

        boolean add(ByteBuffer wrapped, int resultingHash) {
            if (depth == 0) {
                return insertValue(wrapped);
            } else {
                return selectBucket(wrapped, resultingHash);
            }
        }


        boolean delete(ByteBuffer wrapped, int resultingHash) {
            if (depth == 0) {
                return deleteValue(wrapped);
            } else {
                return selectBucketDelete(wrapped, resultingHash);
            }
        }

        private boolean selectBucket(ByteBuffer wrapped, int resultingHash) {
            if (children == null) {
                this.children = new Node[breadth];
            }

            int bucket = Math.abs(resultingHash % children.length);
            // lazy node creation too
            if (children[bucket] == null) {
                children[bucket] = new Node((byte) (depth - 1), breadth, this);
            }
            //
            return children[bucket].add(wrapped, resultingHash - bucket);
        }

        private boolean selectBucketDelete(ByteBuffer wrapped, int resultingHash) {
            if (children == null) {
                return false;
            }

            int bucket = Math.abs(resultingHash % children.length);
            if (children[bucket] == null) {
                return false;
            }

            return children[bucket].delete(wrapped, resultingHash - bucket);
        }

        private synchronized boolean insertValue(ByteBuffer wrapped) {
            if (this.content == null) {
                this.content = new ConcurrentSkipListSet<>();
            }

            boolean res = content.add(wrapped);

            if (res) {
                //compute hash of content
                recomputeHash();
            }
            return res;
        }

        private void recomputeHash() {
            ByteBuffer hashes = ByteBuffer.allocate(content.size() * Integer.BYTES);
            for (ByteBuffer bf : content) {
                hashes.putInt(MurmurHash.hash(bf));
            }
            hash = MurmurHash.hash(hashes);
            if (parent != null) {
                parent.updateHash();
            }
        }

        private synchronized boolean deleteValue(ByteBuffer wrapped) {

            if (this.content == null || !this.content.contains(wrapped)) {
                return false;
            }

            boolean res = this.content.remove(wrapped);
            if (res) {
                recomputeHash();
            }
            return res;
        }

        @Override
        public void updateHash() {

            if (valueCount() == 0) {
                hash = 0;
            } else {
                hash = computeHashFor(children);
            }
            if (parent != null) {
                parent.updateHash();
            }
        }

        @Override
        public byte position(NodeWithUpdateHashAndChildPosition child) {
            return MerkleTreeVariantRoot.position(children, child);
        }

        @Override
        public void path(ByteArrayOutputStream sb) {
            parent.path(sb);
            sb.write(parent.position(this));
        }

        int valueCount() {
            if (content != null) {
                return content.size();
            } else {

                int cnt = 0;
                if (children != null) {
                    for (int i = 0; i < children.length; i++) {
                        cnt += children[i] != null ? children[i].valueCount() : 0;
                    }
                }
                return cnt;

            }
        }

        void export(List<ExportLeaf> export) {
            ByteArrayOutputStream sb = new ByteArrayOutputStream();
            path(sb);
            if (depth == 0) {
                export.add(new ExportLeaf(hash, content == null ? 0 : content.size(), sb));
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
