package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.es.writer.WriteEvent;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;

public class EventUtils {

    public static ByteBuffer create(long sequence, String stream, int version, String data) {
        WriteEvent event = new WriteEvent();
        event.stream = stream;
        event.version = version;
        event.data = StringUtils.toUtf8Bytes(data);
        event.expectedVersion = -1;
        event.type = "TEST";

        ByteBuffer buffer = ByteBuffer.allocate(Event.sizeOf(event));
        Event.serialize(event, sequence, buffer);
        buffer.flip();
        return buffer;
    }

}
