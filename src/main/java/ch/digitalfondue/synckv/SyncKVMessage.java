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
            e.printStackTrace();
        }
    }

    public static class RequestForSyncPayload extends SyncKVMessage implements Serializable {
    }

    public static class SyncPayloadToLeader extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;

        public SyncPayloadToLeader(List<TableMetadata> metadata) {
            this.metadata = metadata;
        }

        @Override
        public String toString() {
            return String.format("SyncPayloadToLeader{metadata=%s}", metadata);
        }
    }



    public static class SyncPayload extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;
        final Set<String> fullSync;

        public SyncPayload(List<TableMetadata> metadata, Set<String> fullSync) {
            this.metadata = metadata;
            this.fullSync = fullSync;
        }

        @Override
        public String toString() {
            return String.format("SyncPayload{metadata=%s, fullSync=%s}", metadata, fullSync);
        }

        Set<String> getRemoteTables() {
            return metadata.stream().map(TableMetadata::getName).collect(Collectors.toSet());
        }

        public TableMetadata getMetadataFor(String name) {
            return metadata.stream().filter(s -> s.name.equals(name)).findFirst().orElse(null);
        }
    }

    public static class SyncPayloadFrom extends SyncKVMessage implements Serializable {

        final Map<String, List<TableAddress>> addressesAndTables = new HashMap<>();

        public SyncPayloadFrom(List<TableAddress> tables) {
            tables.stream().forEach(ta -> {
                if(!addressesAndTables.containsKey(ta.addressEncoded)) {
                    addressesAndTables.put(ta.addressEncoded, new ArrayList<>());
                }
                addressesAndTables.get(ta.addressEncoded).add(ta);
            });
        }

        @Override
        public String toString() {
            return String.format("SyncPayloadFrom {addressesAndTables=%s}", addressesAndTables);
        }
    }

    public static class TableAddress implements Serializable {
        final String table;
        final String addressEncoded;
        final boolean fullSync;

        public TableAddress(String table, Address address, boolean fullSync) {
            this.table = table;
            this.addressEncoded = Utils.addressToBase64(address);
            this.fullSync = fullSync;
        }

        @Override
        public String toString() {
            return String.format("TableAddress{table=%s, address=%s, fullSync=%s}", table, Utils.fromBase64(addressEncoded).toString(), Boolean.toString(fullSync));
        }
    }

    public static class DataToSync extends SyncKVMessage implements Serializable {
        final String name;
        final Map<String, PayloadAndTime> payload;

        public DataToSync(String name) {
            this.name = name;
            this.payload = new HashMap<>();
        }
    }

    public static class PayloadAndTime implements Serializable {
        final byte[] payload;
        final long time;

        public PayloadAndTime(byte[] payload, long time) {
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
