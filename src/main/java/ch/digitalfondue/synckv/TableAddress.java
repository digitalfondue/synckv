package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableAddress implements Serializable {
    final String table;
    final String addressEncoded;

    TableAddress(String table, String addressEncoded) {
        this.table = table;
        this.addressEncoded = addressEncoded;
    }

    @Override
    public String toString() {
        return String.format("TableAddress{table: %s, addressEncoded: %s}", table, addressEncoded);
    }
}
