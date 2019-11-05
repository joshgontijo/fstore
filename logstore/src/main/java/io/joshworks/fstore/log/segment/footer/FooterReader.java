package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;

import java.util.List;
import java.util.stream.Collectors;

public class FooterReader {

    private final DataStream stream;
    private final FooterMap map;

    public FooterReader(DataStream stream, FooterMap map) {
        this.stream = stream;
        this.map = map;
    }

    public <T> T read(String name, Serializer<T> serializer) {
        long position = map.get(name);
        if (FooterMap.NONE == position) {
            return null;
        }
        return readInternal(position, serializer);
    }

    public <T> List<T> findAll(String prefix, Serializer<T> serializer) {
        return map.findAll(prefix)
                .stream()
                .sorted()
                .map(pos -> readInternal(pos, serializer))
                .collect(Collectors.toList());
    }


    public long length() {
        return stream.length();
    }

    private <T> T readInternal(long position, Serializer<T> serializer) {
        RecordEntry<T> recordEntry = stream.read(Direction.FORWARD, position, serializer);
        if (recordEntry == null) {
            throw new IllegalStateException("Could not read mapped footer item at position: " + position);
        }
        return recordEntry.entry();
    }

}
