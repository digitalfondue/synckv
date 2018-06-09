package ch.digitalfondue.synckv;

import org.h2.mvstore.MVStore;
import org.jgroups.JChannel;

import java.security.SecureRandom;

public class SyncKV {

    final JChannel channel;
    final MVStore store;
    final long seed;

    public SyncKV(String fileName, String password, JChannel channel) {
        this.channel = channel;
        MVStore.Builder builder = new MVStore.Builder().fileName(fileName);
        if(password != null) {
            builder.encryptionKey(password.toCharArray());
        }

        this.seed = new SecureRandom().nextLong();

        this.store = builder.open();
    }


    public SyncKVTable getTable(String name) {
        return new SyncKVTable(name, store, seed);
    }
}
