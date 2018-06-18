package ch.digitalfondue.synckv;

import java.io.Serializable;

final class KV implements Serializable {
    final byte[] k;
    final byte[] v;

    KV(byte[] k, byte[] v) {
        this.k = k;
        this.v = v;
    }
}
