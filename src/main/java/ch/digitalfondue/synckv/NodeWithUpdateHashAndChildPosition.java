package ch.digitalfondue.synckv;

interface NodeWithUpdateHashAndChildPosition {
    void updateHash();
    byte position(NodeWithUpdateHashAndChildPosition child);
}
