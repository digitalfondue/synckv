package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.jgroups.Address;
import org.jgroups.JChannel;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public abstract class SyncKVMessage {

    static void broadcast(JChannel jChannel, SyncKVMessage msg) {
        send(jChannel, null, msg);
    }

    static void send(JChannel jChannel, Address address, SyncKVMessage msg) {
        try {
            jChannel.send(address, msg);
        } catch (Exception e) {
        }
    }


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

    public static class SinglePut extends SyncKVMessage implements Serializable {
        final String name;
        final String key;
        final byte[] payload;

        public SinglePut(String name, String key, byte[] payload) {
            this.name = name;
            this.key = key;
            this.payload = payload;
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
