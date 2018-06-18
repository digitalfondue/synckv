package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAndPartialTreeData implements Serializable {
    final int keyCount;
    final int hash;

    TableAndPartialTreeData(int keyCount, int hash) {
        this.keyCount = keyCount;
        this.hash = hash;
    }
}
