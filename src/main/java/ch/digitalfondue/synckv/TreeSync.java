package ch.digitalfondue.synckv;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class TreeSync implements NodeWithChildPosition {

    private final Node[] children;
    private final byte depth;
    private int count;

    TreeSync(byte depth, byte breadth) {
        this.children = new Node[breadth];
        this.depth = depth;
    }

    @Override
    public byte position(NodeWithChildPosition child) {
        return 0;
    }

    @Override
    public void path(ByteArrayOutputStream l) {
    }

    public int getCount() {
        return count;
    }

    void add(byte[] value) {
        count++;
        ByteBuffer wrapped = ByteBuffer.wrap(value);
        int hashWrappedValue = MurmurHash.hash(wrapped);
        int bucket = Math.abs(hashWrappedValue % children.length);

        if (children[bucket] == null) {
            children[bucket] = new Node((byte) (depth - 1), (byte) children.length, this);
        }

        children[bucket].add(wrapped, hashWrappedValue - bucket);
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


    private static class Node implements NodeWithChildPosition {

        private Node[] children;
        private final byte depth;
        private final byte breadth;
        private NodeWithChildPosition parent;
        private int hash;
        private int count;

        Node(byte depth, byte breadth, NodeWithChildPosition parent) {
            this.depth = depth;
            this.breadth = breadth;
            this.parent = parent;
        }

        @Override
        public byte position(NodeWithChildPosition child) {
            return 0;
        }

        @Override
        public void path(ByteArrayOutputStream l) {
        }

        boolean add(ByteBuffer wrapped, int resultingHash) {
            count++;
            if (depth == 0) {
                return insertValue(wrapped);
            } else {
                return selectBucket(wrapped, resultingHash);
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

        private boolean insertValue(ByteBuffer wrapped) {
            hash = MurmurHash.hash(wrapped.array(), hash);
            return true;
        }

        int getHash() {
            return hash;
        }

        void export(List<ExportLeaf> export) {
            ByteArrayOutputStream sb = new ByteArrayOutputStream();
            path(sb);
            if (depth == 0) {
                export.add(new ExportLeaf(getHash(), count, sb));
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

    static class ExportLeaf implements Serializable {
        final int hash;
        final int keyCount;
        final byte[] path;

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
            if (obj == null || !(obj instanceof MerkleTreeVariantRoot.ExportLeaf)) {
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
}
