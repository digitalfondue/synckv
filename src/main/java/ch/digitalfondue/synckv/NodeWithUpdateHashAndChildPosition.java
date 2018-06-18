package ch.digitalfondue.synckv;

import java.io.ByteArrayOutputStream;

interface NodeWithUpdateHashAndChildPosition {
    void updateHash();

    byte position(NodeWithUpdateHashAndChildPosition child);

    void path(ByteArrayOutputStream l);
}
