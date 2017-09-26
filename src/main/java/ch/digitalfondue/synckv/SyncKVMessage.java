package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class SyncKVMessage {


    public static class SyncPayload extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;

        public SyncPayload(List<TableMetadata> metadata) {
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            return String.format("SyncPayload{metadata=%s}", metadata);
        }

        Set<String> getRemoteTables() {
            return metadata.stream().map(TableMetadata::getName).collect(Collectors.toSet());
        }

        public TableMetadata getMetadataFor(String name) {
            return metadata.stream().filter(s -> s.name.equals(name)).findFirst().orElse(null);
        }
    }

    public static class DataToSync extends SyncKVMessage implements Serializable {
        final String name;
        final Map<String, byte[]> payload;

        public DataToSync(String name) {
            this.name = name;
            this.payload = new HashMap<>();
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

        @Override
        public String toString() {
            return String.format("TableMetadata{name=%s,count=%d,bloomFilterSize=%d}", name, count, bloomFilter == null ? -1 : bloomFilter.length);
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
