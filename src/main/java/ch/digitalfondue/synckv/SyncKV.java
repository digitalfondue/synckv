package ch.digitalfondue.synckv;

import org.h2.mvstore.MVStore;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.Protocol;

import java.io.Closeable;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SyncKV implements AutoCloseable, Closeable {

    static {
        ensureProtocol();
    }

    private final SecureRandom random;
    private final JChannel channel;
    private final MVStore store;
    private final RpcFacade rpcFacade;
    private final Map<String, MerkleTreeVariantRoot> syncMap = new ConcurrentHashMap<>();
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    final AtomicBoolean disableSync = new AtomicBoolean();

    /**
     * Note: if you are using this constructor, call SyncKV.ensureProtocol(); before building the JChannel!
     *
     * @param fileName
     * @param password
     * @param channel
     */
    public SyncKV(String fileName, String password, JChannel channel, String channelName) {

        this.channel = channel;
        MVStore.Builder builder = new MVStore.Builder().fileName(fileName);
        if (password != null) {
            builder.encryptionKey(password.toCharArray());
        }

        this.random = new SecureRandom();

        this.store = builder.open();

        ensureSyncMap();


        if (channel != null) {
            try {
                channel.connect(channelName);
                this.rpcFacade = new RpcFacade(this);
                this.rpcFacade.setRpcDispatcher(new RpcDispatcher(channel, rpcFacade));


                this.scheduledExecutor = new ScheduledThreadPoolExecutor(1);

                this.scheduledExecutor.scheduleAtFixedRate(new SynchronizationHandler(this, rpcFacade), 2, 10, TimeUnit.SECONDS);

            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            this.rpcFacade = null;
            this.scheduledExecutor = null;
        }
    }

    public SyncKV(String fileName, String password, String channelName) {
        this(fileName, password, buildChannel(password), channelName);
    }

    public SyncKV(String fileName, String password) {
        this(fileName, password, buildChannel(password), "syncKV");
    }

    public boolean hasTable(String name) {
        return store.hasMap(name);
    }

    public synchronized SyncKVTable getTable(String name) {
        if (!syncMap.containsKey(name)) {
            syncMap.put(name, buildTree());
        }
        return new SyncKVTable(name, store, random, rpcFacade, channel, syncMap.get(name), disableSync);
    }

    public static void ensureProtocol() {
        if (ClassConfigurator.getProtocolId(SymEncryptWithKeyFromMemory.class) == 0) {
            ClassConfigurator.addProtocol((short) 1024, SymEncryptWithKeyFromMemory.class);
        }
    }

    private static MerkleTreeVariantRoot buildTree() {
        //3**7 = 2187 buckets
        return new MerkleTreeVariantRoot((byte) 3, (byte) 7);
    }

    private void ensureSyncMap() {
        for (String name : store.getMapNames()) {
            MerkleTreeVariantRoot tree = buildTree();
            syncMap.put(name, tree);
            Map<byte[], byte[]> map = store.openMap(name);
            map.keySet().stream().forEach(tree::add);
        }
    }


    // for programmatic configuration, imported from https://github.com/belaban/JGroups/blob/master/src/org/jgroups/demos/ProgrammaticChat.java
    // switched to TCP_NIO2 and MPING, will need some tweak?
    static protected JChannel buildChannel(String password) {
        try {
            List<Protocol> protocols = new ArrayList<>();
            protocols.addAll(Arrays.asList(new TCP_NIO2(),
                    new MPING(),
                    new MERGE3(),
                    new FD_SOCK(),
                    new FD_ALL(),
                    new VERIFY_SUSPECT(),
                    new BARRIER()));
            if (password != null) {
                protocols.add(new SymEncryptWithKeyFromMemory(password));
            }
            protocols.addAll(Arrays.asList(
                    new NAKACK2(),
                    new UNICAST3(),
                    new STABLE(),
                    new GMS(),
                    new MFC(),
                    new FRAG2(),
                    new RSVP(),
                    new STATE_TRANSFER()
            ));

            return new JChannel(protocols);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    JChannel getChannel() {
        return channel;
    }


    public String getClusterMemberName() {
        return channel != null ? channel.getAddressAsString() : null;
    }

    public List<String> getClusterMembersName() {
        return channel != null ? channel.view().getMembers().stream().map(Address::toString).collect(Collectors.toList()) : Collections.emptyList();
    }

    List<Address> getClusterMembers() {
        return channel != null ? channel.view().getMembers() : Collections.emptyList();
    }

    public boolean isLeader() {
        return channel != null ? channel.getView().getMembers().get(0).equals(channel.getAddress()) : true;
    }

    Address getAddress() {
        return channel.getAddress();
    }

    Map<String, TableAndPartialTreeData> getTableMetadataForSync() {
        Map<String, TableAndPartialTreeData> res = new HashMap<>();

        syncMap.forEach((k, v) -> {
            res.put(k, new TableAndPartialTreeData(v.getKeyCount(), v.getHash()));
        });
        return res;
    }

    @Override
    public void close() {
        store.close();
        channel.close();
        scheduledExecutor.shutdown();
    }

    MerkleTreeVariantRoot getTableTree(String table) {
        return syncMap.get(table);
    }
}
