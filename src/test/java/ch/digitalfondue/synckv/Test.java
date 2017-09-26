package ch.digitalfondue.synckv;

import java.util.Random;
import java.util.UUID;

public class Test {

    public static void main(String[] args) throws Exception {
        SyncKV kv = new SyncKV("test", "SyncKV");
        SyncKV.SyncKVTable table = kv.getTable("attendees");

        Random r = new Random();
        byte[] payload = new byte[322];
        for(int i = 0; i < 10000; i++) {
            r.nextBytes(payload);
            table.put(UUID.randomUUID().toString(), payload);
            System.out.println(i);
        }
        kv.commit();


        SyncKV k2 = new SyncKV("test2", "SyncKV");

    }
}
