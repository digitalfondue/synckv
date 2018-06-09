package ch.digitalfondue.synckv;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.nio.charset.StandardCharsets;

public class SyncKVTest {

    public static void main(String[] args) throws Exception {

        //for programmatic configuration, imported from https://github.com/belaban/JGroups/blob/master/src/org/jgroups/demos/ProgrammaticChat.java
        // switched to TCP_NIO2 and MPING
        // TODO: custom encryption provider for loading the encryption key from memory and not from  a keystore
        Protocol[] prot_stack = {
                new TCP_NIO2(),
                new MPING(),
                new MERGE3(),
                new FD_SOCK(),
                new FD_ALL(),
                new VERIFY_SUSPECT(),
                new BARRIER(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new UFC(),
                new MFC(),
                new FRAG2()};
        JChannel channel = new JChannel(prot_stack).name("SyncKV");


        channel.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View new_view) {
                System.out.println("view: " + new_view);
            }

            public void receive(Message msg) {
                System.out.println("<< " + msg.getObject() + " [" + msg.getSrc() + "]");
            }
        });

        channel.connect("SyncKV");


        SyncKV syncKV = new SyncKV(null, null, channel);

        SyncKVTable table = syncKV.getTable("test");

        table.put("my_key", "my value 1".getBytes(StandardCharsets.UTF_8));
        byte[] res1 = table.get("my_key");

        System.err.println("res1 is " + new String(res1, StandardCharsets.UTF_8));

        table.put("my_key", "my value 2".getBytes(StandardCharsets.UTF_8));
        byte[] res2 = table.get("my_key");
        System.err.println("res2 is " + new String(res2, StandardCharsets.UTF_8));

        for (; ; ) {
            String line = Util.readStringFromStdin(": ");
            channel.send(null, line);
        }
    }
}
