package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class BulkBufferRef implements Supplier<ByteBuffer>, AutoCloseable {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private ByteBuffer buffer;
    private final BufferPool pool;
    private final int[] markers;
    private final int[] lengths;
    private final int entries;
    private int i = 0;

    private BulkBufferRef(ByteBuffer buffer, BufferPool pool, int[] markers, int[] lengths, int entries) {
        this.buffer = buffer;
        this.pool = pool;
        this.markers = markers;
        this.lengths = lengths;
        this.entries = entries;
    }

    public static BulkBufferRef ofEmpty(ByteBuffer buffer, BufferPool pool) {
        buffer.position(0).limit(0);
        return withMarker(buffer, pool, new int[0], new int[0], 0);
    }

    public static BulkBufferRef of(ByteBuffer buffer, BufferPool pool) {
        Objects.requireNonNull(buffer);
        int position = buffer.position();
        int len = buffer.remaining();
        return new BulkBufferRef(buffer, pool, new int[]{position}, new int[]{len}, 1);
    }

    public static BulkBufferRef withMarker(ByteBuffer buffer, BufferPool pool, int[] markers, int[] lengths, int entries) {
        Objects.requireNonNull(buffer);
        return new BulkBufferRef(buffer, pool, markers, lengths, entries);
    }

    private ByteBuffer next() {
        if (i >= entries) {
            return EMPTY;
        }
        buffer.limit(markers[i] + lengths[i]);
        buffer.position(markers[i]);
        i++;
        return buffer;
    }

    public <T> int[] readAllInto(Collection<T> col, Serializer<T> serializer) {
        int[] readLengths = new int[entries];
        for (int j = i; j < entries; j++) {
            ByteBuffer bb = next();
            if (bb.hasRemaining()) {
                T entry = serializer.fromBytes(bb);
                col.add(entry);
                readLengths[j] = lengths[j] + RecordHeader.HEADER_OVERHEAD;
            }
        }
        return readLengths;
    }

    public boolean hasNext() {
        return i < markers.length;
    }

    //TODO list can be avoided
    public List<Integer> lengths() {
        List<Integer> len = new ArrayList<>();
        for (int j = 0; j < entries; j++) {
            len.add(lengths[j]);
        }
        return len;
    }

    @Override
    public ByteBuffer get() {
        if (!hasNext()) {
            return EMPTY;
        }
        buffer.limit(markers[0] + lengths[0]);
        buffer.position(markers[0]);
        return buffer;
    }

    @Override
    public void close() {
        ByteBuffer buf = this.buffer;
        this.buffer = null;
        if (pool != null) {
            pool.free(buf);
        }
    }
}