package io.joshworks.es2.writer;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;

import java.nio.ByteBuffer;

public class IndexBufferedWriter {

    public static void write(SegmentChannel channel, ByteBuffer event, ByteBuffer buffer, long logAddress) {

        long stream = Event.stream(event);
        int version = Event.version(event);
        int eventSize = Event.sizeOf(event);

        if(buffer.)


    }

}
