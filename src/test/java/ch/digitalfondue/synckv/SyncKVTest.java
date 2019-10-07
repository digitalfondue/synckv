package ch.digitalfondue.synckv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SyncKVTest {

    public static void main(String[] args) {

        //Logger rootLog = Logger.getLogger("");
        //rootLog.setLevel( Level.FINE );
        //rootLog.getHandlers()[0].setLevel( Level.FINE );

        removeFile("s1");
        removeFile("s2");
        removeFile("s3");

        SyncKV kv = new SyncKV("s1", "SyncKV");
        kv.disableSync(true);
        SyncKVTable table = kv.getTable("attendees");

        Random r = new Random();

        AtomicInteger keyGenerator = new AtomicInteger();

        for (int i = 0; i < 10_000; i++) {

            String key = Integer.toString(keyGenerator.incrementAndGet());
            System.err.println("adding in kv with key " + key);
            table.put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }

        kv.getTable("anothertable").put("test", "value".getBytes(StandardCharsets.UTF_8));


        SyncKV k2 = new SyncKV("s2", "SyncKV");
        k2.disableSync(true);

        SyncKV k3 = new SyncKV("s3", "SyncKV");
        k3.disableSync(true);

        for (int i = 0; i < 50_000; i++) {
            String key = Integer.toString(keyGenerator.incrementAndGet());
            boolean choice = r.nextBoolean();
            System.err.println("adding in k" + (choice ? "2" : "3") + " with key " + key);
            (choice ? k2 : k3).getTable("attendees").put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }

        kv.disableSync(false);
        k2.disableSync(false);
        k3.disableSync(false);


        if (true) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {

                System.err.println("members of the cluster: " + kv.getClusterMembersName());
                System.err.println("keys in kv[attendees] " + kv.getClusterMemberName() + " " + kv.getTable("attendees").count());
                System.err.println("keys in k2[attendees] " + k2.getClusterMemberName() + " " + k2.getTable("attendees").count());
                System.err.println("keys in k3[attendees] " + k3.getClusterMemberName() + " " + k3.getTable("attendees").count());

                System.err.println("keys in kv[anothertable] " + kv.getClusterMemberName() + " " + kv.getTable("anothertable").count());
                if (k2.hasTable("anothertable")) {
                    System.err.println("keys in k2[anothertable] " + k2.getClusterMemberName() + " " + k2.getTable("anothertable").count());
                }
                if (k3.hasTable("anothertable")) {
                    System.err.println("keys in k3[anothertable] " + k3.getClusterMemberName() + " " + k3.getTable("anothertable").count());
                }


                String key = Integer.toString(keyGenerator.incrementAndGet());
                String value = "hello world " + keyGenerator.get();
                if (r.nextBoolean()) {

                    System.err.println("adding in kv with key " + key + " and value " + value);
                    table.put(key, value.getBytes(StandardCharsets.UTF_8));

                    byte[] res = (r.nextBoolean() ? k2 : k3).getTable("attendees").get(key);

                    String toFormat = res != null ? new String(res, StandardCharsets.UTF_8) : null;
                    System.err.println("trying to fetch distributed get " + toFormat);
                } else {
                    System.err.println("adding in kv2 with key " + key + " and value " + value);
                    k2.getTable("attendees").put(key, value.getBytes(StandardCharsets.UTF_8));
                }


            }, 20, 20, TimeUnit.SECONDS);
        }
    }

    private static void removeFile(String name) {
        File f = new File(name);
        if (f.exists()) {
            f.delete();
        }
    }
}
