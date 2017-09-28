package ch.digitalfondue.synckv;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {

    public static void main(String[] args) throws Exception {


        SyncKV kv = new SyncKV("test", "SyncKV");
        SyncKV.SyncKVTable table = kv.getTable("attendees");

        Random r = new Random();

        AtomicInteger keyGenerator = new AtomicInteger();

        for (int i = 0; i < 10; i++) {

            String key = Integer.toString(keyGenerator.getAndIncrement());
            System.err.println("adding in kv with key " + key);
            table.put(key, ("hello world " + i).getBytes(StandardCharsets.UTF_8));
        }
        kv.commit();

        SyncKV k2 = new SyncKV("test2", "SyncKV");
        SyncKV k3 = new SyncKV("test3", "SyncKV");

        if (true) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {


                System.err.println("keys in kv " + new ArrayList<>(kv.getTable("attendees").table.keySet()));
                System.err.println("keys in k2 " + new ArrayList<>(k2.getTable("attendees").table.keySet()));
                System.err.println("keys in k3 " + new ArrayList<>(k3.getTable("attendees").table.keySet()));

                if (r.nextBoolean()) {
                    String key = Integer.toString(keyGenerator.getAndIncrement());
                    System.err.println("adding in kv with key " + key);
                    table.put(key, "hello world 11".getBytes(StandardCharsets.UTF_8));
                } else {
                    String key = Integer.toString(keyGenerator.getAndIncrement());
                    System.err.println("adding in kv2 with key " + key);
                    k2.getTable("attendees").put(key, "hello world 11".getBytes(StandardCharsets.UTF_8));
                }


            }, 20, 20, TimeUnit.SECONDS);
        }

    }
}
