package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.MerkleTreeVariantRoot.ExportLeaf;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RpcFacade {

    private final static Logger LOGGER = Logger.getLogger(RpcFacade.class.getName());

    private final SyncKV syncKV;
    private RpcDispatcher rpcDispatcher;

    public RpcFacade(SyncKV syncKV) {
        this.syncKV = syncKV;
    }

    void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
        this.rpcDispatcher = rpcDispatcher;
    }

    private Address getCurrentAddress() {
        return syncKV.getChannel().getAddress();
    }

    // --- PUT ---------

    void putRequest(Address source, String table, byte[] key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall("handlePutRequest", new Object[]{Utils.addressToBase64(source), table, key, value}, new Class[]{String.class, String.class, byte[].class, byte[].class}));
    }

    public void handlePutRequest(String src, String table, byte[] key, byte[] value) {

        Address source = Utils.fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncKV.getTable(table).addRawKV(key, value);
    }
    // -----------------


    // --- GET ---------

    private static final Comparator<KV> DESC_METADATA_ORDER = Comparator.<KV, ByteBuffer>comparing(payload -> ByteBuffer.wrap(payload.k)).reversed();

    KV getValue(Address src, String table, String key) {
        JChannel channel = syncKV.getChannel();
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());

        if (everybodyElse.isEmpty()) {
            return null;
        }

        MethodCall call = new MethodCall("handleGetValue", new Object[]{Utils.addressToBase64(src), table, key}, new Class[]{String.class, String.class, String.class});
        KV res = null;
        try {
            RspList<KV> rsps = rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.SYNC().setTimeout(50));

            //fetch the value with the "biggest" metadata concatenate(insertion_time,seed)
            res = rsps.getResults()
                    .stream()
                    .filter(payload -> payload != null && payload.k != null)
                    .sorted(DESC_METADATA_ORDER)
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling getValue", e);
        }
        return res;
    }

    public KV handleGetValue(String src, String table, String key) {
        Address address = Utils.fromBase64(src);
        if (address.equals(getCurrentAddress())) {
            return null;
        } else {
            return syncKV.getTable(table).get(key, false);
        }
    }

    // --- STEP 1 ------
    CompletableFuture<Map<String, TableStats>> getTableMetadataForSync(Address address) {
        return syncSend(address, new MethodCall("handleGetTableMetadataForSync", new Object[]{}, new Class[]{}));
    }

    public Map<String, TableStats> handleGetTableMetadataForSync() {
        return syncKV.getTableMetadataForSync();
    }

    // -----------------
    // -----------------
    CompletableFuture<List<KV>> getFullTableData(Address address, String tableName) {
        return syncSend(address, new MethodCall("handleGetFullTableData", new Object[]{tableName}, new Class[]{String.class}));
    }

    public List<KV> handleGetFullTableData(String tableName) {
        return syncKV.getTable(tableName).exportRawData();
    }

    // -----------------
    CompletableFuture<List<KV>> getPartialTableData(Address address, String tableName, List<ExportLeaf> exportLeaves) {
        return syncSend(address, new MethodCall("handleGetPartialTableData", new Object[]{tableName, exportLeaves}, new Class[]{String.class, List.class}));
    }

    // We receive the leaf from the remote, if we remove them (the equals one) from our local tree, we have the
    // difference, thus the bucket that need to be sent.
    //
    // Note: it's unidirectional, but due to the nature of the sync process, it will converge
    public List<KV> handleGetPartialTableData(String tableName, List<ExportLeaf> remote) {
        List<KV> res = new ArrayList<>();
        MerkleTreeVariantRoot tableTree = syncKV.getTableTree(tableName);
        SyncKVTable localTable = syncKV.getTable(tableName);
        Set<ExportLeaf> local = new HashSet<>(tableTree.exportLeafStructureOnly());
        local.removeAll(remote);
        for (ExportLeaf el : local) {
            tableTree.getKeysForPath(el.getPath()).forEach(bb -> {
                byte[] rawKey = bb.array();
                res.add(new KV(rawKey, localTable.getRawKV(rawKey)));
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
}
