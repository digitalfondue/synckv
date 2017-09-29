package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAddress implements Serializable {
    final String table;
    final String addressEncoded;
    final boolean fullSync;

    TableAddress(String table, String addressEncoded, boolean fullSync) {
        this.table = table;
        this.addressEncoded = addressEncoded;
        this.fullSync = fullSync;
    }
}
