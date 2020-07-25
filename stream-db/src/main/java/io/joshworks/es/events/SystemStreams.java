package io.joshworks.es.events;

import io.joshworks.es.index.IndexKey;
import io.joshworks.es.writer.WriteEvent;

import java.nio.charset.StandardCharsets;

public class SystemStreams {

    private SystemStreams() {

    }

    private static final String LINKTO_TYPE = ">";

    public static WriteEvent linkTo(String srcStream, int srcVersion, String dstStream, int dstVersion) {
        WriteEvent event = new WriteEvent();
        event.stream = dstStream;
        event.version = dstVersion;
        event.type = LINKTO_TYPE;
        event.timestamp = System.currentTimeMillis();
        event.metadata = new byte[0];
        event.data = IndexKey.toString(srcStream, srcVersion).getBytes(StandardCharsets.UTF_8);
        return event;
    }

}
