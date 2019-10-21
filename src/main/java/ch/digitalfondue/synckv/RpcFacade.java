package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Internal use.
 * RPC facade for inter node communication.
 */
class RpcFacade {

    private final static Logger LOGGER = Logger.getLogger(RpcFacade.class.getName());

    private final RpcDispatcher rpcDispatcher;
    private final JChannel channel;
    private final Function<String, SyncKVTable> tableSupplier;
    private final Supplier<Map<String, TableStats>> statsSupplier;

    RpcFacade(JChannel channel, Function<String, SyncKVTable> tableSupplier, Supplier<Map<String, TableStats>> statsSupplier) {
        this.channel = channel;
        this.rpcDispatcher = new RpcDispatcher(channel, this);
        this.rpcDispatcher.setMethodInvoker(this::methodInvoker);
        this.tableSupplier = tableSupplier;
        this.statsSupplier = statsSupplier;
    }

    Object methodInvoker(Object target, short method_id, Object[] args) {
        Object[] argumentsValue = (Object[]) args[0];
        switch (method_id) {
            case HANDLE_PUT_REQUEST:
                handlePutRequest((String) argumentsValue[0], (String) argumentsValue[1], (byte[]) argumentsValue[2], (byte[]) argumentsValue[3]);
                break;
            case HANDLE_GET_VALUE:
                return handleGetValue((String) argumentsValue[0], (String) argumentsValue[1], (String) argumentsValue[2]);
            case HANDLE_GET_TABLE_METADATA_FOR_SYNC:
                return handleGetTableMetadataForSync();
            case HANDLE_GET_FULL_TABLE_DATA:
                handleGetFullTableData((String) argumentsValue[0], (String) argumentsValue[1]);
                break;
            case HANDLE_GET_PARTIAL_TABLE_DATA:
                handleGetPartialTableData((String) argumentsValue[0], (List<TreeSync.ExportLeaf>) argumentsValue[1], (String) argumentsValue[2]);
                break;
            case HANDLE_RAW_BULK_PUT:
                setHandleRawBulkPut((String) argumentsValue[0], (List<KV>) argumentsValue[1]);
                break;
        }
        return null;
    }

    private Address getCurrentAddress() {
        return channel.getAddress();
    }

    // --- PUT ---------

    void putRequest(String table, byte[] key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall(HANDLE_PUT_REQUEST, new Object[]{addressToBase64(getCurrentAddress()), table, key, value},
                        new Class[]{String.class, String.class, byte[].class, byte[].class}));
    }

    private static final short HANDLE_PUT_REQUEST = 0;
    void handlePutRequest(String src, String table, byte[] key, byte[] value) {

        Address source = fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        tableSupplier.apply(table).addRawKV(key, value);
    }
    // -----------------


    // --- GET ---------

    private static final Comparator<KV> DESC_METADATA_ORDER = (kv1, kv2) -> SyncKVTable.compareKey(kv1.k, kv2.k);

    KV getValue(String table, String key) {
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(getCurrentAddress())).collect(Collectors.toList());

        if (everybodyElse.isEmpty()) {
            return null;
        }

        MethodCall call = new MethodCall(HANDLE_GET_VALUE, new Object[]{addressToBase64(getCurrentAddress()), table, key}, new Class[]{String.class, String.class, String.class});
        KV res = null;
        try {
            RspList<KV> rsps = rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.SYNC().setTimeout(50));

            //fetch the value with the "biggest" metadata concatenate(insertion_time,seed)
            res = rsps.getResults()
                    .stream()
                    .filter(payload -> payload != null && payload.k != null)
                    .sorted(DESC_METADATA_ORDER.reversed())
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling getValue", e);
        }
        return res;
    }

    private static final short HANDLE_GET_VALUE = 1;
    KV handleGetValue(String src, String table, String key) {
        Address address = fromBase64(src);
        if (address.equals(getCurrentAddress())) {
            return null;
        } else {
            return tableSupplier.apply(table).get(key, false);
        }
    }

    // --- STEP 1 ------
    CompletableFuture<Map<String, TableStats>> getTableMetadataForSync(Address address) {
        return syncSend(address, new MethodCall(HANDLE_GET_TABLE_METADATA_FOR_SYNC, new Object[]{}, new Class[]{}));
    }

    private static final short HANDLE_GET_TABLE_METADATA_FOR_SYNC = 2;
    Map<String, TableStats> handleGetTableMetadataForSync() {
        return statsSupplier.get();
    }

    // -----------------
    // -----------------
    void getFullTableData(Address address, String tableName, Address source) {
        send(address, new MethodCall(HANDLE_GET_FULL_TABLE_DATA, new Object[]{tableName, addressToBase64(source)}, new Class[]{String.class, String.class}));
    }

    private static final short HANDLE_GET_FULL_TABLE_DATA = 3;
    void handleGetFullTableData(String tableName, String src) {
        Address toSend = fromBase64(src);
        SyncKVTable table = tableSupplier.apply(tableName);
        List<KV> res = new ArrayList<>();
        Iterator<byte[]> it = table.rawKeys();
        while (it.hasNext()) {
            byte[] k = it.next();
            byte[] v = table.getRawKV(k);
            if (v != null) {
                res.add(new KV(k, v));
            }
            if (res.size() > 250) {
                send(toSend, new MethodCall(HANDLE_RAW_BULK_PUT, new Object[]{tableName, res}, new Class[]{String.class, List.class}));
                res = new ArrayList<>();
            }
        }

        if (res.size() > 0) {
            send(toSend, new MethodCall(HANDLE_RAW_BULK_PUT, new Object[]{tableName, res}, new Class[]{String.class, List.class}));
        }
    }

    // -----------------
    void getPartialTableData(Address address, String tableName, List<TreeSync.ExportLeaf> exportLeaves, Address source) {
        send(address, new MethodCall(HANDLE_GET_PARTIAL_TABLE_DATA, new Object[]{tableName, exportLeaves, addressToBase64(source)}, new Class[]{String.class, List.class, String.class}));
    }

    // We receive the leaf from the remote, if we remove them (the equals one) from our local tree, we have the
    // difference, thus the bucket that need to be sent.
    //
    // Note: it's unidirectional, but due to the nature of the sync process, it will converge
    private static final short HANDLE_GET_PARTIAL_TABLE_DATA = 4;
    void handleGetPartialTableData(String tableName, List<TreeSync.ExportLeaf> remote, String source) {
        Address toSend = fromBase64(source);
        SyncKVTable localTable = tableSupplier.apply(tableName);
        TreeSync localTreeSync = localTable.getTreeSync();
        localTreeSync.removeMatchingLeafs(remote);
        List<KV> res = new ArrayList<>();
        Iterator<byte[]> it = localTable.rawKeys();
        while(it.hasNext()) {
            byte[] key = it.next();
            if (localTreeSync.isInExistingBucket(key)) {
                byte[] value = localTable.getRawKV(key);
                if (value != null) {
                    res.add(new KV(key, localTable.getRawKV(key)));
                }
            }
            if (res.size() > 250) {
                send(toSend, new MethodCall(HANDLE_RAW_BULK_PUT, new Object[]{tableName, res}, new Class[]{String.class, List.class}));
                res = new ArrayList<>();
            }
        }
        if (res.size() > 0) {
            send(toSend, new MethodCall(HANDLE_RAW_BULK_PUT, new Object[]{tableName, res}, new Class[]{String.class, List.class}));
        }
    }
    // -----------------

    private static final short HANDLE_RAW_BULK_PUT = 5;

    void setHandleRawBulkPut(String tableName, List<KV> kvs) {
        SyncKVTable localTable = tableSupplier.apply(tableName);
        localTable.importRawData(kvs);
        LOGGER.log(Level.FINE, () -> "Bulk import of " + kvs.size() + " keys in table " + tableName);
    }
    // -----------------



    private void broadcastToEverybodyElse(MethodCall call) {
        try {
            List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(getCurrentAddress())).collect(Collectors.toList());
            if (everybodyElse.isEmpty()) {
                return;
            }
            rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling broadcastToEverybodyElse", e);
        }
    }

    private void send(Address address, MethodCall call) {
        try {
            rpcDispatcher.callRemoteMethod(address, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling send", e);
        }
    }

    private <T> CompletableFuture<T> syncSend(Address address, MethodCall call) {
        try {
            return rpcDispatcher.callRemoteMethodWithFuture(address, call, RequestOptions.SYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling syncSend", e);
            return null;
        }
    }
    // -----------------

    static Address fromBase64(String encoded) {
        try {
            return Util.readAddress(new ByteArrayDataInputStream(Base64.getDecoder().decode(encoded)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String addressToBase64(Address address) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream();
        try {
            Util.writeAddress(address, out);
            return Base64.getEncoder().encodeToString(out.getByteBuffer().array());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
