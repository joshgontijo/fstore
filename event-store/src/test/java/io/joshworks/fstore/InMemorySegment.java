package io.joshworks.fstore;

import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.header.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class InMemorySegment<T> implements Log<T> {

    public final List<T> records = new ArrayList<>();

    @Override
    public String name() {
        return "mem-segment";
    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        return null;
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        final Queue<T> copy = new ArrayDeque<>(records);
        return new SegmentIterator<>() {
            @Override
            public boolean endOfLog() {
                return copy.isEmpty();
            }

            @Override
            public long position() {
                return 0;
            }

            @Override
            public void close() {
                copy.clear();
            }

            @Override
            public boolean hasNext() {
                return !copy.isEmpty();
            }

            @Override
            public T next() {
                return copy.poll();
            }
        };
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public T read(long position) {
        return records.get((int) position);
    }

    @Override
    public long physicalSize() {
        return 0;
    }

    @Override
    public long logicalSize() {
        return 0;
    }

    @Override
    public long dataSize() {
        return 0;
    }

    @Override
    public long actualDataSize() {
        return 0;
    }

    @Override
    public long uncompressedDataSize() {
        return 0;
    }

    @Override
    public long headerSize() {
        return 0;
    }

    @Override
    public long footerSize() {
        return 0;
    }

    @Override
    public long remaining() {
        return 0;
    }

    @Override
    public void delete() {

    }

    @Override
    public void roll(int level, boolean trim) {

    }

    @Override
    public boolean readOnly() {
        return false;
    }

    @Override
    public boolean closed() {
        return false;
    }

    @Override
    public long entries() {
        return records.size();
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public long created() {
        return 0;
    }

    @Override
    public long uncompressedSize() {
        return 0;
    }

    @Override
    public Type type() {
        return null;
    }

    @Override
    public Metrics metrics() {
        return null;
    }

    @Override
    public long append(T data) {
        int size = records.size();
        records.add(data);
        return size;
    }

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }
}