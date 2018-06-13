package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAndPartialTreeData implements Serializable {
    private final String name;
    private final int[] treeData;

    TableAndPartialTreeData(String name, int[] treeData) {
        this.name = name;
        this.treeData = treeData;
    }
}
