package io.joshworks.es.log;

import io.joshworks.es.Event;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EventUtils {

    public static ByteBuffer create(long sequence, long stream, int version, String data) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return Event.create(sequence, stream, version, ByteBuffer.wrap(bytes));
    }

}
