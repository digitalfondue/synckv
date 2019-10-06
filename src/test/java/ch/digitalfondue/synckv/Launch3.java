package ch.digitalfondue.synckv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Launch3 {

    public static void main(String[] args) {
        File f = new File("launch3");
        if (f.exists()) {
            f.delete();
        }
        SyncKV k3 = new SyncKV("launch3", "SyncKV");
        k3.disableSync.set(true);

        AtomicInteger keyGenerator = new AtomicInteger();

        for (int i = 25_000; i < 50_000; i++) {
            String key = "k3"+ keyGenerator.incrementAndGet();
            k3.getTable("attendees").put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }

        k3.disableSync.set(false);

        if (true) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {
                System.err.println("members of the cluster: " + k3.getClusterMembersName());
                System.err.println("key count in k3[attendees] " + k3.getClusterMemberName() + " " + k3.getTable("attendees").count());
                //System.err.println("keys in k3[attendees] " + k3.getClusterMemberName() + " " + k3.getTable("attendees").keySet());
                System.err.println("keys in k3[anothertable] " + k3.getClusterMemberName() + " " + k3.getTable("anothertable").count());

                String key = "k3" + keyGenerator.incrementAndGet();
                k3.getTable("attendees").put(key, (key+"hello world ").getBytes(StandardCharsets.UTF_8));

            }, 20, 20, TimeUnit.SECONDS);
        }
    }
}
