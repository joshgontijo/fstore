package io.joshworks.es;

import io.joshworks.es.events.WriteEvent;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;

public class EventUtils {

    public static ByteBuffer create(String stream, String type, int version, String data) {
        WriteEvent wevent = new WriteEvent(stream, type, version, StringUtils.toUtf8Bytes(data), Buffers.EMPTY_BYTES);
        ByteBuffer dst = ByteBuffer.allocate(1024);
        Event.serialize(wevent, version, 1, dst);
        return dst.flip();
    }

}
