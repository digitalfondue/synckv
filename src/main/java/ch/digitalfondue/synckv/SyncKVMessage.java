package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

abstract class SyncKVMessage implements Serializable {


    final String src;

    //for serialization!
    public SyncKVMessage() {
        this.src = null;
    }

    protected SyncKVMessage(String src) {
        this.src = src;
    }

    static void broadcastToEverybodyElse(RpcDispatcher rpcDispatcher, SyncKVMessage msg) {
        try {
            rpcDispatcher.callRemoteMethods(null, "receive", new Object[]{msg}, new Class[]{SyncKVMessage.class}, RequestOptions.ASYNC());
        } catch (Exception e) {
            //FIXME use java logging
            e.printStackTrace();
        }
    }

    static void broadcastToEverybodyElse(JChannel channel, RpcDispatcher rpcDispatcher, MethodCall call) {
        try {
            List<Address> everybodyElse = channel.view().getMembers().stream().filter(address-> !address.equals(channel.getAddress())).collect(Collectors.toList());
            rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            //FIXME use java logging
            e.printStackTrace();
        }
    }

    static void send(RpcDispatcher rpcDispatcher, Address address, MethodCall call) {
        try {
            rpcDispatcher.callRemoteMethod(address, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            //FIXME use java logging
            e.printStackTrace();
        }
    }

    static void send(RpcDispatcher rpcDispatcher, Address address, SyncKVMessage msg) {
        try {
            rpcDispatcher.callRemoteMethod(address, "receive", new Object[] {msg}, new Class[]{SyncKVMessage.class}, RequestOptions.ASYNC());
        } catch (Exception e) {
            //FIXME use java logging
            e.printStackTrace();
        }
    }

    static class RequestForSyncPayload extends SyncKVMessage implements Serializable {
        RequestForSyncPayload(String src) {
            super(src);
        }
    }

    static class SyncPayloadToLeader extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;

        SyncPayloadToLeader(String src, List<TableMetadata> metadata) {
            super(src);
            this.metadata = metadata;
        }

        TableMetadata getMetadataFor(String name) {
            return metadata.stream().filter(s -> s.name.equals(name)).findFirst().orElse(null);
        }
    }

    static class SyncPayload extends SyncKVMessage implements Serializable {
        final List<TableMetadata> metadata;
        final Set<String> fullSync;

        SyncPayload(String src, List<TableMetadata> metadata, Set<String> fullSync) {
            super(src);
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

        DataToSync(String src, String name) {
            super(src);
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
