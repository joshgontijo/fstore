package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.log.record.DataStream;

import java.nio.ByteBuffer;

public class FooterWriter {

    private final DataStream stream;
    private final long start;

    public FooterWriter(DataStream stream) {
        this.start = stream.position();
        this.stream = stream;
    }

    public long write(ByteBuffer data) {
        return stream.write(data);
    }

    public long write(ByteBuffer[] data) {
        return stream.write(data);
    }

    public long position() {
        return stream.position();
    }

    public void position(long position) {
        if (position < start) {
            throw new IllegalArgumentException("Position cannot be less than " + start);
        }
        stream.position(position);
    }

    public long length() {
        return position() - start;
    }

    public long start() {
        return start;
    }
}
