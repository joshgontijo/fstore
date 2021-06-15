package io.joshworks.es;

import io.joshworks.es.events.WriteEvent;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EventHelper {

    public static WriteEvent evOf(String stream, int expectedVersion, String evType, String data) {
        return new WriteEvent(
                stream,
                evType,
                expectedVersion,
                data.getBytes(StandardCharsets.UTF_8),
                new byte[0]);

    }

    public static ByteBuffer evOf(long sequence, String stream, int version, String data) {
        WriteEvent event = evOf(stream, -1, "TEST", data);

        ByteBuffer buffer = ByteBuffer.allocate(Event.sizeOf(event));
        Event.serialize(event, version, sequence, buffer);
        buffer.flip();
        return buffer;
    }
}
