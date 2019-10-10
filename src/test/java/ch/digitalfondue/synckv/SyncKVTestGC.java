package ch.digitalfondue.synckv;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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

            long counterValue = counter.get();
            if (r.nextBoolean()) {
                test.put("counter", "" + counterValue);
                System.err.println("added in test value " + counterValue);
                System.err.println("fetched value is " + test.getAsString("counter"));
            } else {
                test2.put("counter", "" + counterValue);
                System.err.println("added in test2 value " + counterValue);
                System.err.println("fetched value is " + test2.getAsString("counter"));
            }
            System.err.println("counter value is " + counterValue);
            System.err.println("members of the cluster: " + kv.getClusterMembersName());
            System.err.println("keys in kv[attendees] " + kv.getClusterMemberName() + dumpTable(test));
            System.err.println("keys in k2[attendees] " + kv2.getClusterMemberName() + dumpTable(test2));
            System.err.println("Values of counters are: " + test.getAsString("counter") + ", " + test2.getAsString("counter"));

            counter.incrementAndGet();


        }, 20, 10, TimeUnit.SECONDS);
    }

    public static String dumpTable(SyncKVTable table) {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, byte[]> k : getKeysWithRawKey(table)) {
            sb.append("{").append(k.getKey()).append(", ").append(new String(table.getRawKV(k.getValue()), StandardCharsets.UTF_8)).append("} ");
        }
        return sb.toString();
    }

    static List<Map.Entry<String, byte[]>> getKeysWithRawKey(SyncKVTable table) {
        return table.rawKeySet().stream().map(s -> {
            String res = new String(s, 0, s.length - SyncKVTable.METADATA_LENGTH, StandardCharsets.UTF_8);
            return new AbstractMap.SimpleImmutableEntry<>(res, s);
        }).collect(Collectors.toList());
    }
}
