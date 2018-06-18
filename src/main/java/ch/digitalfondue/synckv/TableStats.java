package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableStats implements Serializable {
    final int keyCount;
    final int hash;

    TableStats(int keyCount, int hash) {
        this.keyCount = keyCount;
        this.hash = hash;
    }
}
