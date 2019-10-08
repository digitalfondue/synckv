package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.MerkleTreeVariantRoot.ExportLeaf;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Internal use.
 * RPC facade for inter node communication.
 */
class RpcFacade {

    private final static Logger LOGGER = Logger.getLogger(RpcFacade.class.getName());

    private final SyncKV syncKV;
    private final RpcDispatcher rpcDispatcher;

    RpcFacade(SyncKV syncKV) {
        this.syncKV = syncKV;
        this.rpcDispatcher = new RpcDispatcher(syncKV.getChannel(), this);
        this.rpcDispatcher.setMethodInvoker(this::methodInvoker);
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
                return handleGetFullTableData((String) argumentsValue[0]);
            case HANDLE_GET_PARTIAL_TABLE_DATA:
                return handleGetPartialTableData((String) argumentsValue[0], (List<ExportLeaf>) argumentsValue[1]);
        }
        return null;
    }

    private Address getCurrentAddress() {
        return syncKV.getChannel().getAddress();
    }

    // --- PUT ---------

    void putRequest(Address source, String table, byte[] key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall(HANDLE_PUT_REQUEST, new Object[]{addressToBase64(source), table, key, value}, new Class[]{String.class, String.class, byte[].class, byte[].class}));
    }

    private static final short HANDLE_PUT_REQUEST = 0;
    void handlePutRequest(String src, String table, byte[] key, byte[] value) {

        Address source = fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncKV.getTable(table).addRawKV(key, value);
    }
    // -----------------


    // --- GET ---------

    private static final Comparator<KV> DESC_METADATA_ORDER = (kv1, kv2) -> SyncKVTable.compareKey(kv1.k, kv2.k);

    KV getValue(Address src, String table, String key) {
        JChannel channel = syncKV.getChannel();
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());

        if (everybodyElse.isEmpty()) {
            return null;
        }

        MethodCall call = new MethodCall(HANDLE_GET_VALUE, new Object[]{addressToBase64(src), table, key}, new Class[]{String.class, String.class, String.class});
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
            return syncKV.getTable(table).get(key, false);
        }
    }

    // --- STEP 1 ------
    CompletableFuture<Map<String, TableStats>> getTableMetadataForSync(Address address) {
        return syncSend(address, new MethodCall(HANDLE_GET_TABLE_METADATA_FOR_SYNC, new Object[]{}, new Class[]{}));
    }

    private static final short HANDLE_GET_TABLE_METADATA_FOR_SYNC = 2;
    Map<String, TableStats> handleGetTableMetadataForSync() {
        return syncKV.getTableMetadataForSync();
    }

    // -----------------
    // -----------------
    CompletableFuture<List<KV>> getFullTableData(Address address, String tableName) {
        return syncSend(address, new MethodCall(HANDLE_GET_FULL_TABLE_DATA, new Object[]{tableName}, new Class[]{String.class}));
    }

    private static final short HANDLE_GET_FULL_TABLE_DATA = 3;
    List<KV> handleGetFullTableData(String tableName) {
        return syncKV.getTable(tableName).exportRawData();
    }

    // -----------------
    CompletableFuture<List<KV>> getPartialTableData(Address address, String tableName, List<ExportLeaf> exportLeaves) {
        return syncSend(address, new MethodCall(HANDLE_GET_PARTIAL_TABLE_DATA, new Object[]{tableName, exportLeaves}, new Class[]{String.class, List.class}));
    }

    // We receive the leaf from the remote, if we remove them (the equals one) from our local tree, we have the
    // difference, thus the bucket that need to be sent.
    //
    // Note: it's unidirectional, but due to the nature of the sync process, it will converge
    private static final short HANDLE_GET_PARTIAL_TABLE_DATA = 4;
    List<KV> handleGetPartialTableData(String tableName, List<ExportLeaf> remote) {
        List<KV> res = new ArrayList<>();
        SyncKVTable localTable = syncKV.getTable(tableName);
        MerkleTreeVariantRoot tableTree = syncKV.getMerkleTreeForMap(localTable);
        Set<ExportLeaf> local = new HashSet<>(tableTree.exportLeafStructureOnly());
        local.removeAll(remote);
        for (ExportLeaf el : local) {
            tableTree.getKeysForPath(el.getPath()).forEach(bb -> {
                byte[] rawKey = bb.array();
                byte[] data = localTable.getRawKV(rawKey);
                res.add(new KV(rawKey, data));
            });
        }
        return res;
    }
    // -----------------


    private void broadcastToEverybodyElse(MethodCall call) {
        try {
            JChannel channel = syncKV.getChannel();
            List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());
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
