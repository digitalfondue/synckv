package ch.digitalfondue.synckv;

import java.io.Serializable;

class TableMetadataWithHashedBloomFilter implements Serializable {

    final String name;
    final int count;
    final byte[] hashedBloomFilter;


    TableMetadataWithHashedBloomFilter(String name, int count, byte[] hashedBloomFilter) {
        this.name = name;
        this.count = count;
        this.hashedBloomFilter = hashedBloomFilter;
    }

    String getName() {
        return name;
    }
}
