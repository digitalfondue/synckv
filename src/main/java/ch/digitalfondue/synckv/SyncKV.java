package ch.digitalfondue.synckv;

import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SyncKV {

    final JChannel channel;
    final MVStore store;
    final long seed;

    static {
        ensureProtocol();
    }

    /**
     * Note: if you are using this constructor, call SyncKV.ensureProtocol(); before building the JChannel!
     *
     * @param fileName
     * @param password
     * @param channel
     */
    public SyncKV(String fileName, String password, JChannel channel) {

        this.channel = channel;
        MVStore.Builder builder = new MVStore.Builder().fileName(fileName);
        if (password != null) {
            builder.encryptionKey(password.toCharArray());
        }

        this.seed = new SecureRandom().nextLong();

        this.store = builder.open();

        if (channel != null) {
            try {
                channel.connect("SyncKV");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public SyncKV(String fileName, String password) {
        this(fileName, password, buildChannel(password));
    }

    public static void ensureProtocol() {
        if (ClassConfigurator.getProtocolId(SymEncryptWithKeyFromMemory.class) == 0) {
            ClassConfigurator.addProtocol((short) 1024, SymEncryptWithKeyFromMemory.class);
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
            if(password != null) {
                protocols.add(new SymEncryptWithKeyFromMemory(password));
            }
            protocols.addAll(Arrays.asList(
                    new NAKACK2(),
                    new UNICAST3(),
                    new STABLE(),
                    new GMS(),
                    new MFC(),
                    new FRAG2()
            ));

            return new JChannel(protocols).name("SyncKV");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    public SyncKVTable getTable(String name) {
        return new SyncKVTable(name, store, seed);
    }
}
