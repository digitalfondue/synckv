package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableMetadata implements Serializable {

    final String name;
    final MerkleTreeVariantRoot.ExportLeaf[] tableMetadata;

    TableMetadata(String name, MerkleTreeVariantRoot.ExportLeaf[] tableMetadata) {
        this.name = name;
        this.tableMetadata = tableMetadata;
    }
}
