package ch.digitalfondue.synckv;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SyncKVTestGC {

    public static void main(String[] args) {
        Logger rootLog = Logger.getLogger("");
        rootLog.setLevel( Level.FINE );
        rootLog.getHandlers()[0].setLevel( Level.FINE );

        SyncKV kv = new SyncKV(null, "SyncKV");
        SyncKV kv2 = new SyncKV(null, "SyncKV");

        AtomicLong counter = new AtomicLong(0);


        SyncKVTable test = kv.getTable("test");
        SyncKVTable test2 = kv.getTable("test");

        Random r = new Random();

        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {

            if (r.nextBoolean()) {
                test.put("counter", "" + counter.get());
            } else {
                test2.put("counter", "" + counter.get());
            }

            counter.incrementAndGet();

            System.err.println("members of the cluster: " + kv.getClusterMembersName());
            System.err.println("keys in kv[attendees] " + kv.getClusterMemberName() + new ArrayList<>(kv.getTable("test").rawKeySet()));
            System.err.println("keys in k2[attendees] " + kv2.getClusterMemberName() + new ArrayList<>(kv2.getTable("test").rawKeySet()));


        }, 20, 10, TimeUnit.SECONDS);
    }
}
