package ch.digitalfondue.synckv;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Map;

public class StructuredTableTest {

    public static class Attendee {
        private final String firstname;
        private final String lastname;
        private final String email;

        Attendee(String firstname, String lastname, String email) {
            this.firstname = firstname;
            this.lastname = lastname;
            this.email = email;
        }
    }

    private static void to(Attendee attendee, DataOutputStream daos) throws IOException {
        daos.writeUTF(attendee.firstname);
        daos.writeUTF(attendee.lastname);
        daos.writeUTF(attendee.email);
    }

    private static Attendee from(DataInputStream dis) throws IOException {
        return new Attendee(dis.readUTF(), dis.readUTF(), dis.readUTF());
    }

    @Test
    public void sampleWorkflowTest() {
        try (SyncKV kv = new SyncKV(null, null, null, null)) {

            SyncKVStructuredTable<Attendee> attendeeTable = kv.getTable("attendee").toStructured(Attendee.class, StructuredTableTest::from, StructuredTableTest::to);

            attendeeTable.put("test1", new Attendee("1f", "1l", "1@"));
            attendeeTable.put("test2", new Attendee("2f", "2l", "2@"));


            Attendee a = attendeeTable.stream().filter(e -> e.getValue().lastname.equals("1l")).map(Map.Entry::getValue).findFirst().orElseThrow(IllegalStateException::new);
            Assert.assertEquals("1f", a.firstname);
            Assert.assertEquals("1l", a.lastname);
            Assert.assertEquals("1@", a.email);
        }
    }
}
