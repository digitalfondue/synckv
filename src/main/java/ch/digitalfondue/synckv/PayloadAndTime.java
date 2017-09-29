package ch.digitalfondue.synckv;

import java.io.Serializable;

class PayloadAndTime implements Serializable {
    final byte[] payload;
    final long time;

    PayloadAndTime(byte[] payload, long time) {
        this.payload = payload;
        this.time = time;
    }
}
