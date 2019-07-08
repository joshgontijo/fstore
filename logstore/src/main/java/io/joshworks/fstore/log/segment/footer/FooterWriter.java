package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.log.record.DataStream;

import java.nio.ByteBuffer;

public class FooterWriter {

    private final DataStream stream;
    private final FooterMap map;

    public FooterWriter(DataStream stream, FooterMap map) {
        this.stream = stream;
        this.map = map;
    }

    public int write(String name, ByteBuffer data) {
        return map.write(name, stream, data);
    }

    public int write(String name, ByteBuffer[] data) {
        return map.write(name, stream, data);
    }

    public long position() {
        return stream.position();
    }
}
