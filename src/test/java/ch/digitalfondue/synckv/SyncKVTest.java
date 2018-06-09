package ch.digitalfondue.synckv;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.nio.charset.StandardCharsets;

public class SyncKVTest {

    public static void main(String[] args) throws Exception {
        SyncKV syncKV = new SyncKV(null, "my_password");

        syncKV.channel.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("view: " + new_view);
            }

            public void receive(Message msg) {
                System.out.println("<< " + msg.getObject() + " [" + msg.getSrc() + "]");
            }
        });

        SyncKVTable table = syncKV.getTable("test");

        table.put("my_key", "my value 1".getBytes(StandardCharsets.UTF_8));
        byte[] res1 = table.get("my_key");

        System.err.println("res1 is " + new String(res1, StandardCharsets.UTF_8));

        table.put("my_key", "my value 2".getBytes(StandardCharsets.UTF_8));
        byte[] res2 = table.get("my_key");
        System.err.println("res2 is " + new String(res2, StandardCharsets.UTF_8));

        for (; ; ) {
            String line = Util.readStringFromStdin(": ");
            syncKV.channel.send(null, line);
        }
    }
}
