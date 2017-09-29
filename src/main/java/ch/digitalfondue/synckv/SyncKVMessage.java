package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

abstract class SyncKVMessage  {



    static class TableAddress implements Serializable {
        final String table;
        final String addressEncoded;
        final boolean fullSync;

        TableAddress(String table, String addressEncoded, boolean fullSync) {
            this.table = table;
            this.addressEncoded = addressEncoded;
            this.fullSync = fullSync;
        }
    }

    static class PayloadAndTime implements Serializable {
        final byte[] payload;
        final long time;

        PayloadAndTime(byte[] payload, long time) {
            this.payload = payload;
            this.time = time;
        }
    }

    static class TableMetadata implements Serializable {

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
}
