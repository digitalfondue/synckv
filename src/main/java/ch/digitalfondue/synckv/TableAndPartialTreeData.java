package ch.digitalfondue.synckv;

import java.io.Serializable;
import java.util.Arrays;

class TableAndPartialTreeData implements Serializable {
    final String name;
    final int keyCount;
    final int[] treeData;

    TableAndPartialTreeData(String name, int keyCount, int[] treeData) {
        this.name = name;
        this.keyCount = keyCount;
        this.treeData = treeData;
    }
}
