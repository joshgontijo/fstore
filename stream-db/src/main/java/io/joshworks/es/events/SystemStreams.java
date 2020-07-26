package io.joshworks.es.events;

import io.joshworks.es.Event;
import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SystemStreams {

    private SystemStreams() {

    }

    private static final String LINKTO_TYPE = ">";
    private static final String INDEX_FLUSH_TYPE = "$INDEX_FLUSH";

    public static final String INDEX_STREAM = "$INDEX";

    public static boolean isLinkTo(ByteBuffer buffer) {
        return LINKTO_TYPE.equals(Event.eventType(buffer));
    }

    public static boolean isIndexFlush(ByteBuffer buffer) {
        return INDEX_FLUSH_TYPE.equals(Event.eventType(buffer));
    }

    public static WriteEvent linkTo(String srcStream, int srcVersion, String dstStream, int expectedVersion) {
        WriteEvent event = new WriteEvent();
        event.stream = dstStream;
        event.type = LINKTO_TYPE;
        event.metadata = Buffers.EMPTY_BYTES;
        event.expectedVersion = expectedVersion;
        event.data = IndexKey.toString(srcStream, srcVersion).getBytes(StandardCharsets.UTF_8);
        return event;
    }

    public static WriteEvent indexFlush() {
        WriteEvent event = new WriteEvent();
        event.stream = INDEX_STREAM;
        event.type = INDEX_FLUSH_TYPE;
        event.metadata = Buffers.EMPTY_BYTES;
        event.data = Buffers.EMPTY_BYTES;
        return event;
    }

}
