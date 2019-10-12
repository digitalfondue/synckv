package ch.digitalfondue.synckv;

import org.h2.mvstore.MVStore;
import org.jgroups.Address;
import org.jgroups.JChannel;
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

/**
 * Represent the key-value store.
 */
public class SyncKV implements AutoCloseable, Closeable {

    static {
        ensureProtocol();
    }

    private final SecureRandom random;
    private final JChannel channel;
    private final MVStore store;
    private final RpcFacade rpcFacade;
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    private final AtomicBoolean disableSync = new AtomicBoolean();
    private final AtomicBoolean disableCompacting = new AtomicBoolean();
    private final OldKVCollector oldKVCollector;
    private final Map<String, SyncKVTable> tables = new ConcurrentHashMap<>();

    /**
     * Note: if you are using this constructor, call SyncKV.ensureProtocol(); before building the JChannel!
     *
     * @param fileName the db file, pass null for an in memory representation
     * @param password password for encrypting the file _and_ the communication between nodes. Pass null if you want to ignore this option.
     * @param channel custom {@link JChannel} configuration. Pass null if you don't need the sync feature, which can be useful for local data.
     * @param channelName name of the channel.
     */
    public SyncKV(String fileName, String password, JChannel channel, String channelName) {

        this.channel = channel;
        MVStore.Builder builder = new MVStore.Builder().fileName(fileName);
        if (password != null) {
            builder.encryptionKey(password.toCharArray());
        }

        this.random = new SecureRandom();

        this.store = builder.open();

        if (channel != null) {
            try {
                channel.connect(channelName);
                this.rpcFacade = new RpcFacade(this);

                this.scheduledExecutor = new ScheduledThreadPoolExecutor(2);

                this.scheduledExecutor.scheduleAtFixedRate(new SynchronizationHandler(this, rpcFacade), 2, 10, TimeUnit.SECONDS);

            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            this.rpcFacade = null;
            this.scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        }
        this.oldKVCollector = new OldKVCollector(this);
        this.scheduledExecutor.scheduleAtFixedRate(oldKVCollector, 2, 5, TimeUnit.MINUTES);
    }

    /**
     * Create a SyncKV instance with default settings. A {@link JChannel} will be built using a tcp_nio2+mping stack with the specified channel name.
     *
     * @param fileName fileName the db file, pass null for an in memory representation
     * @param password password for encrypting the file _and_ the communication between nodes. Pass null if you want to ignore this option.
     * @param channelName name of the channel.
     */
    public SyncKV(String fileName, String password, String channelName) {
        this(fileName, password, buildChannel(password), channelName);
    }

    /**
     * Create a SyncKV instance with default settings. A {@link JChannel} will be built using a tcp_nio2+mping stack with the channelName "syncKV".
     *
     * @param fileName fileName the db file, pass null for an in memory representation
     * @param password password for encrypting the file _and_ the communication between nodes. Pass null if you want to ignore this option.
     */
    public SyncKV(String fileName, String password) {
        this(fileName, password, buildChannel(password), "syncKV");
    }

    /**
     * Disable/Enable the removal of old key/values.
     *
     * @param disableCompacting
     */
    public void disableCompacting(boolean disableCompacting) {
        this.disableCompacting.set(disableCompacting);
    }

    /**
     * Disable/Enable the synchronization.
     *
     * @param disableSync
     */
    public void disableSync(boolean disableSync) {
        this.disableSync.set(disableSync);
    }

    public boolean isCompactingDisabled() {
        return disableCompacting.get();
    }

    public boolean isSyncDisabled() {
        return disableSync.get();
    }

    public boolean hasTable(String name) {
        return store.hasMap(name);
    }

    public synchronized SyncKVTable getTable(String name) {
        if (tables.containsKey(name)) {
            return tables.get(name);
        }
        SyncKVTable kv = new SyncKVTable(name, store, random, rpcFacade, channel, disableSync);
        tables.put(name, kv);
        return kv;
    }

    public Set<String> getTableNames() {
        return store.getMapNames();
    }

    public static void ensureProtocol() {
        if (ClassConfigurator.getProtocolId(SymEncryptWithKeyFromMemory.class) == 0) {
            ClassConfigurator.addProtocol((short) 1024, SymEncryptWithKeyFromMemory.class);
        }

        if (ClassConfigurator.getProtocolId(MPINGCustom.class) == 0) {
            ClassConfigurator.addProtocol((short) 1025, MPINGCustom.class);
        }
    }

    MerkleTreeVariantRoot getMerkleTreeForMap(SyncKVTable table) {
        //3**7 = 2187 buckets
        MerkleTreeVariantRoot t = new MerkleTreeVariantRoot((byte) 3, (byte) 7);
        table.rawKeySet().stream().forEach(t::add);
        return t;
    }

    private static class MPINGCustom extends MPING {
        MPINGCustom(boolean send_on_all_interfaces) {
            this.send_on_all_interfaces = send_on_all_interfaces;
        }
    }

    // for programmatic configuration, imported from https://github.com/belaban/JGroups/blob/master/src/org/jgroups/demos/ProgrammaticChat.java
    // switched to TCP_NIO2 and MPING, will need some tweak? -> edit: now reswitched to TCP, used custom mping as we need to send on all interfaces
    static protected JChannel buildChannel(String password) {
        try {
            List<Protocol> protocols = new ArrayList<>();
            protocols.addAll(Arrays.asList(new TCP(),
                    new MPINGCustom(true),
                    new MERGE3(),
                    new FD_SOCK(),
                    new FD_ALL2(),
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

    Map<String, TableStats> getTableMetadataForSync() {
        Map<String, TableStats> res = new HashMap<>();
        tables.forEach((mapName, table) -> {
            TableStats stats = table.getTableStats();
            res.put(mapName, stats);
        });
        return res;
    }

    @Override
    public void close() {
        oldKVCollector.run();
        store.close();
        if (channel != null) {
            channel.close();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
    }
}
