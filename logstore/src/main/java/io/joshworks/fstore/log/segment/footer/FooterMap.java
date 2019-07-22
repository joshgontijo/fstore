package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.serializer.Serializers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FooterMap {

    private final Serializer<Map<String, Long>> mapSerializer = Serializers.mapSerializer(Serializers.VSTRING, Serializers.LONG);
    private final Map<String, Long> items = new ConcurrentHashMap<>();
    static final long NONE = -1;

    public long writeTo(DataStream stream) {
        return stream.write(items, mapSerializer);
    }

    public void load(LogHeader header, DataStream stream) {
        if (!header.readOnly() || header.footerSize() == 0) {
            return;
        }

        long mapPosition = header.footerMapPosition();
        RecordEntry<Map<String, Long>> record = stream.read(Direction.FORWARD, mapPosition, mapSerializer);
        if (record.isEmpty()) {
            throw new IllegalStateException("Could not load footer map: Empty footer map");
        }
        items.putAll(record.entry());
    }

    <T> void write(String name, DataStream stream, T entry, Serializer<T> serializer) {
        long position = stream.write(entry, serializer);
        if (items.containsKey(name)) {
            throw new IllegalArgumentException("'" + name + "' already exist in the footer map");
        }
        items.put(name, position);
    }

    List<Long> findAll(String prefix) {
        return items.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(prefix)).map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    long get(String name) {
        return items.getOrDefault(name, NONE);
    }

}
