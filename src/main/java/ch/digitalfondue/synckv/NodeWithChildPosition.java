package ch.digitalfondue.synckv;

import java.io.ByteArrayOutputStream;

interface NodeWithChildPosition {

    byte position(NodeWithChildPosition child);

    void path(ByteArrayOutputStream l);
}
