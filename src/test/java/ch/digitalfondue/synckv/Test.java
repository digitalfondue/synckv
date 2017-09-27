package ch.digitalfondue.synckv;

import ch.digitalfondue.synckv.bloom.CountingBloomFilter;
import ch.digitalfondue.synckv.bloom.Key;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Test {

    public static void main(String[] args) throws Exception {


        SyncKV kv = new SyncKV("test", "SyncKV");
        SyncKV.SyncKVTable table = kv.getTable("attendees");



        for (int i = 0; i < 10; i++) {

            String key = UUID.randomUUID().toString();
            System.err.println("adding in kv with key " + key);
            table.put(key, ("hello world "+i).getBytes(StandardCharsets.UTF_8));
        }
        kv.commit();

        SyncKV k2 = new SyncKV("test2", "SyncKV");
        SyncKV k3 = new SyncKV("test3", "SyncKV");

        if(false) {
            new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(() -> {


                //if (r.nextBoolean()) {
                String key = UUID.randomUUID().toString();
                System.err.println("adding in kv with key " + key);

                table.put(key, "hello world 11".getBytes(StandardCharsets.UTF_8));
            /*} else {
                System.err.println("adding in kv2");
                k2.getTable("attendees").put(UUID.randomUUID().toString(), payload);
            }*/


            }, 20, 20, TimeUnit.SECONDS);
        }

    }
}
