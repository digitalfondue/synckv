package ch.digitalfondue.synckv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Launch2 {

    public static void main(String[] args) {
        File f = new File("launch2");
        if (f.exists()) {
            f.delete();
        }
        SyncKV k2 = new SyncKV("launch2", "SyncKV");
        k2.disableSync(true);

        AtomicInteger keyGenerator = new AtomicInteger();

        for (int i = 0; i < 25_000; i++) {
            String key = "k2" + (keyGenerator.incrementAndGet());
            k2.getTable("attendees").put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }

        k2.disableSync(false);

        if (true) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {
                System.err.println("members of the cluster: " + k2.getClusterMembersName());
                System.err.println("key count in k2[attendees] " + k2.getClusterMemberName() + " " + k2.getTable("attendees").count());
                //System.err.println("keys in k2[attendees] " + k2.getClusterMemberName() + " " + k2.getTable("attendees").keySet());
                System.err.println("keys in k2[anothertable] " + k2.getClusterMemberName() + " " + k2.getTable("anothertable").count());

                String key = "k2" + keyGenerator.incrementAndGet();
                k2.getTable("attendees").put(key, (key+"hello world ").getBytes(StandardCharsets.UTF_8));

            }, 20, 20, TimeUnit.SECONDS);
        }

    }
}
