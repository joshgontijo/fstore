package io.joshworks.es2.writer;

import io.joshworks.es2.SegmentChannel;

import java.nio.ByteBuffer;

public class BufferedWriter {

    public static long write(SegmentChannel channel, ByteBuffer src, ByteBuffer buffer) {
        if (src.remaining() > buffer.capacity()) {
            flush(channel, buffer);
            return channel.append(src);
        }
        if (buffer.remaining() < src.remaining()) {
            flush(channel, buffer);
        }
        int offset = buffer.position();
        buffer.put(src);
        return channel.position() + offset;
    }

    public static void flush(SegmentChannel channel, ByteBuffer buffer) {
        if (buffer.position() > 0) {
            channel.append(buffer.flip());
            buffer.clear();
        }
    }
}
