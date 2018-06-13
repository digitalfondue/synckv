package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAndPartialTreeData implements Serializable {
    private final String name;
    private final int keyCount;
    private final int[] treeData;

    TableAndPartialTreeData(String name, int keyCount, int[] treeData) {
        this.name = name;
        this.keyCount = keyCount;
        this.treeData = treeData;
    }
}
