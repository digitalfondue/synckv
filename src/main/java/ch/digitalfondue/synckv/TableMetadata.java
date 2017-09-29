package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

class TableMetadata implements Serializable {

    final String name;
    final int count;
    final byte[] bloomFilter;

    TableMetadata(String name, int count, byte[] bloomFilter) {
        this.name = name;
        this.count = count;
        this.bloomFilter = bloomFilter;
    }

    String getName() {
        return name;
    }

    CountingBloomFilter getBloomFilter() {
        try {
            if (bloomFilter != null) {
                CountingBloomFilter cbf = Utils.bloomFilterInstance();
                cbf.readFields(new DataInputStream(new ByteArrayInputStream(bloomFilter)));
                return cbf;
            }
        } catch (IOException e) {
        }
        return null;
    }

}
