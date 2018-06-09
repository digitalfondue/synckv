package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;

import java.util.List;
import java.util.Objects;
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

    public void setRpcDispatcher(RpcDispatcher rpcDispatcher) {
        this.rpcDispatcher = rpcDispatcher;
    }

    private Address getCurrentAddress() {
        return syncKV.getChannel().getAddress();
    }

    // --- PUT ---------


    void putRequest(Address source, String table, String key, byte[] value) {
        broadcastToEverybodyElse(new MethodCall("handlePutRequest", new Object[]{Utils.addressToBase64(source), table, key, value}, new Class[]{String.class, String.class, String.class, byte[].class}));
    }

    public void handlePutRequest(String src, String table, String key, byte[] value) {

        Address source = Utils.fromBase64(src);

        if (source.equals(getCurrentAddress())) {
            //calling himself, ignore
            return;
        }

        syncKV.getTable(table).put(key, value, false);
    }
    // -----------------


    // --- GET ---------

    byte[] getValue(Address src, String table, String key) {
        JChannel channel = syncKV.getChannel();
        List<Address> everybodyElse = channel.view().getMembers().stream().filter(address -> !address.equals(channel.getAddress())).collect(Collectors.toList());

        if (everybodyElse.isEmpty()) {
            return null;
        }

        MethodCall call = new MethodCall("handleGetValue", new Object[]{Utils.addressToBase64(src), table, key}, new Class[]{String.class, String.class, String.class});
        byte[] res = null;
        try {
            RspList<byte[]> rsps = rpcDispatcher.callRemoteMethods(everybodyElse, call, RequestOptions.SYNC().setTimeout(50));
            res = rsps.getResults().stream().filter(Objects::nonNull).findFirst().orElse(null);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling getValue", e);
        }
        return res;
    }

    public byte[] handleGetValue(String src, String table, String key) {
        Address address = Utils.fromBase64(src);
        if (address.equals(getCurrentAddress())) {
            return null;
        } else {
            return syncKV.getTable(table).get(key, true);
        }
    }

    // -----------------

    void broadcastToEverybodyElse(MethodCall call) {
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

    void send(Address address, MethodCall call) {
        try {
            rpcDispatcher.callRemoteMethod(address, call, RequestOptions.ASYNC());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error while calling send", e);
        }
    }
}
