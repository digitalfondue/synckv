package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAndPartialTreeData implements Serializable {
    final String name;
    final int keyCount;
    final int hash;

    TableAndPartialTreeData(String name, int keyCount, int hash) {
        this.name = name;
        this.keyCount = keyCount;
        this.hash = hash;
    }
}
