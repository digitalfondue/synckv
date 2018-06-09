package ch.digitalfondue.synckv;

import org.jgroups.Address;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.Base64;

public class Utils {

    static Address fromBase64(String encoded) {
        try {
            return Util.readAddress(new ByteArrayDataInputStream(Base64.getDecoder().decode(encoded)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String addressToBase64(Address address) {
        ByteArrayDataOutputStream out = new ByteArrayDataOutputStream();
        try {
            Util.writeAddress(address, out);
            return Base64.getEncoder().encodeToString(out.getByteBuffer().array());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
