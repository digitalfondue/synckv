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

abstract class SyncKVMessage {

    static void broadcast(JChannel jChannel, SyncKVMessage msg) {
        send(jChannel, null, msg);
    }

    static void send(JChannel jChannel, Address address, SyncKVMessage msg) {
        try {
            jChannel.send(address, msg);
        } catch (Exception e) {
            //FIXME use java logging
            e.printStackTrace();
        }
    }

    static class RequestForSyncPayload extends SyncKVMessage implements Serializable {
    }

    static class SyncPayloadToLeader extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;

        SyncPayloadToLeader(List<TableMetadata> metadata) {
            this.metadata = metadata;
        }

        TableMetadata getMetadataFor(String name) {
            return metadata.stream().filter(s -> s.name.equals(name)).findFirst().orElse(null);
        }
    }



    static class SyncPayload extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;
        final Set<String> fullSync;

        SyncPayload(List<TableMetadata> metadata, Set<String> fullSync) {
            this.metadata = metadata;
            this.fullSync = fullSync;
        }

        Set<String> getRemoteTables() {
            return metadata.stream().map(TableMetadata::getName).collect(Collectors.toSet());
        }

        TableMetadata getMetadataFor(String name) {
            return metadata.stream().filter(s -> s.name.equals(name)).findFirst().orElse(null);
        }
    }

    static class SyncPayloadFrom extends SyncKVMessage implements Serializable {

        final Map<String, List<TableAddress>> addressesAndTables = new HashMap<>();

        SyncPayloadFrom(List<TableAddress> tables) {
            tables.stream().forEach(ta -> {
                if(!addressesAndTables.containsKey(ta.addressEncoded)) {
                    addressesAndTables.put(ta.addressEncoded, new ArrayList<>());
                }
                addressesAndTables.get(ta.addressEncoded).add(ta);
            });
        }
    }

    static class TableAddress implements Serializable {
        final String table;
        final String addressEncoded;
        final boolean fullSync;

        TableAddress(String table, Address address, boolean fullSync) {
            this.table = table;
            this.addressEncoded = Utils.addressToBase64(address);
            this.fullSync = fullSync;
        }
    }

    static class DataToSync extends SyncKVMessage implements Serializable {
        final String name;
        final Map<String, PayloadAndTime> payload;

        DataToSync(String name) {
            this.name = name;
            this.payload = new HashMap<>();
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
