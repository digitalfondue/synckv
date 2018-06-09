package ch.digitalfondue.synckv;

import java.nio.charset.StandardCharsets;

public class SyncKVTest {

    public static void main(String[] args) throws Exception {


        SyncKV syncKV = new SyncKV(null, null, null);

        SyncKVTable table = syncKV.getTable("test");

        table.put("my_key", "my value 1".getBytes(StandardCharsets.UTF_8));
        byte[] res1 = table.get("my_key");

        System.err.println("res1 is " + new String(res1, StandardCharsets.UTF_8));

        table.put("my_key", "my value 2".getBytes(StandardCharsets.UTF_8));
        byte[] res2 = table.get("my_key");
        System.err.println("res2 is " + new String(res2, StandardCharsets.UTF_8));
    }
}
