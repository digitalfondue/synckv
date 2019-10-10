package ch.digitalfondue.synckv;

import org.junit.Test;

public class ReadAndDump {

    //@Test
    public void readAndDumpTest() {
        SyncKV db = new SyncKV("alfio-pi-synckv", "alfio-pi-synckv");

        SyncKVTable scanLog = db.getTable("scan_log");
        Iterable<String> iterable = () -> scanLog.keys();
        iterable.forEach(k -> {
            System.out.println("key is " + k);
            System.out.println("value is " + scanLog.getAsString(k));
        });
    }
}
