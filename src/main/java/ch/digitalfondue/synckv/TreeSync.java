package ch.digitalfondue.synckv;

import java.io.ByteArrayOutputStream;

class TreeSync implements NodeWithChildPosition {

    @Override
    public byte position(NodeWithChildPosition child) {
        return 0;
    }

    @Override
    public void path(ByteArrayOutputStream l) {
    }

    void add(byte[] value) {
    }


    private static class Node implements NodeWithChildPosition {

        @Override
        public byte position(NodeWithChildPosition child) {
            return 0;
        }

        @Override
        public void path(ByteArrayOutputStream l) {
        }
    }
}
