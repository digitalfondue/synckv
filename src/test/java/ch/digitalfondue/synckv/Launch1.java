package ch.digitalfondue.synckv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Launch1 {

    public static void main(String[] args) {
        File f = new File("launch1");
        if (f.exists()) {
            f.delete();
        }
        SyncKV kv = new SyncKV("launch1", "SyncKV");

        kv.disableSync(true);
        SyncKVTable table = kv.getTable("attendees");

        AtomicInteger keyGenerator = new AtomicInteger();

        for (int i = 0; i < 10_000; i++) {

            String key = "kv" + keyGenerator.incrementAndGet();
            System.err.println("adding in kv with key " + key);
            table.put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }

        kv.getTable("anothertable").put("test", "value".getBytes(StandardCharsets.UTF_8));

        kv.disableSync(false);

        if (true) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {
                System.err.println("members of the cluster: " + kv.getClusterMembersName());
                System.err.println("key count in kv[attendees] " + kv.getClusterMemberName() + " " + kv.getTable("attendees").count());
                //System.err.println("keys in kv[attendees] " + kv.getClusterMemberName() + " " + kv.getTable("attendees").keySet());
                System.err.println("keys in kv[anothertable] " + kv.getClusterMemberName() + " " + kv.getTable("anothertable").count());

                String key = "kv" + keyGenerator.incrementAndGet();
                table.put(key, (key+"hello world ").getBytes(StandardCharsets.UTF_8));

            }, 20, 20, TimeUnit.SECONDS);
        }

    }
}
