package io.joshworks.fstore.ie.server.protocol;

import java.nio.ByteBuffer;

public class Message {

    private static final int RESPONSE_ACK = 1;

    public static void writeAck(ByteBuffer dst) {
        dst.putInt(RESPONSE_ACK);
    }


}
