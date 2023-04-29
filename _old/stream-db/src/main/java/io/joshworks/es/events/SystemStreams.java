package io.joshworks.es.events;

import io.joshworks.es.Event;
import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;

public class SystemStreams {

    public static final String INDEX_STREAM = "$INDEX";
    private static final String LINKTO_TYPE = ">";
    private static final String INDEX_FLUSH_TYPE = "$INDEX_FLUSH";

    private SystemStreams() {

    }

    public static boolean isLinkTo(ByteBuffer buffer) {
        return LINKTO_TYPE.equals(Event.eventType(buffer));
    }

    public static boolean isIndexFlush(ByteBuffer buffer) {
        return INDEX_FLUSH_TYPE.equals(Event.eventType(buffer));
    }

    public static WriteEvent linkTo(LinkToEvent ev) {
        String data = IndexKey.toString(ev.srcStream(), ev.srcVersion());
        return new WriteEvent(
                ev.dstStream(),
                LINKTO_TYPE,
                ev.expectedVersion(),
                StringUtils.toUtf8Bytes(data),
                Buffers.EMPTY_BYTES);
    }

    public static WriteEvent indexFlush() {
        return new WriteEvent(
                INDEX_STREAM,
                INDEX_FLUSH_TYPE,
                -1,
                Buffers.EMPTY_BYTES,
                Buffers.EMPTY_BYTES);
    }

}
