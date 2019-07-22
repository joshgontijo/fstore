package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.record.DataStream;

public class FooterWriter {

    private final DataStream stream;
    private final FooterMap map;

    public FooterWriter(DataStream stream, FooterMap map) {
        this.stream = stream;
        this.map = map;
    }

    public <T> int write(String name, T entry, Serializer<T> serializer) {
        return map.write(name, stream, entry, serializer);
    }

    public long position() {
        return stream.position();
    }
}
