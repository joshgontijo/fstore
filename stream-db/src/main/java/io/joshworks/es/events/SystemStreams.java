package io.joshworks.es.events;

import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.charset.StandardCharsets;

public class SystemStreams {

    private SystemStreams() {

    }

    private static final String LINKTO_TYPE = ">";
    private static final String INDEX_FLUSH = "$INDEX_FLUSH";

    private static final String INDEX_STREAM = "$INDEX";

    public static WriteEvent linkTo(String srcStream, int srcVersion, String dstStream) {
        WriteEvent event = new WriteEvent();
        event.stream = dstStream;
        event.type = LINKTO_TYPE;
        event.metadata = Buffers.EMPTY_BYTES;
        event.data = IndexKey.toString(srcStream, srcVersion).getBytes(StandardCharsets.UTF_8);
        return event;
    }

    public static WriteEvent indexFlush() {
        WriteEvent event = new WriteEvent();
        event.stream = INDEX_STREAM;
        event.type = INDEX_FLUSH;
        event.metadata = Buffers.EMPTY_BYTES;
        event.data = Buffers.EMPTY_BYTES;
        return event;
    }

}
