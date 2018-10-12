package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class BufferRef implements Supplier<ByteBuffer>, AutoCloseable {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private ByteBuffer buffer;
    private final BufferPool pool;
    private final int[] markers;
    private final int[] lengths;
    private final int entries;
    private int i = 0;

    private BufferRef(ByteBuffer buffer, BufferPool pool, int[] markers, int[] lengths, int entries) {
        this.buffer = buffer;
        this.pool = pool;
        this.markers = markers;
        this.lengths = lengths;
        this.entries = entries;
    }

    public static BufferRef ofEmpty() {
        return withMarker(ByteBuffer.allocate(0), null, new int[0], new int[0], 0);
    }

    public static BufferRef of(ByteBuffer buffer, BufferPool pool) {
        Objects.requireNonNull(buffer);
        int position = buffer.position();
        int len = buffer.remaining();
        return new BufferRef(buffer, pool, new int[]{position}, new int[] {len}, 1);
    }

    public static BufferRef withMarker(ByteBuffer buffer, BufferPool pool, int[] markers, int[] lengths, int entries) {
        Objects.requireNonNull(buffer);
        BufferRef bufferRef = new BufferRef(buffer, pool, markers, lengths, entries);
        return bufferRef;
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
        if(!hasNext()) {
            return EMPTY;
        }
        buffer.limit(markers[0] + lengths[0]);
        buffer.position(markers[0]);
        return buffer;
    }

    public void clear() {
        ByteBuffer buf = this.buffer;
        this.buffer = null;
        if (pool != null) {
            pool.free(buf);
        }
    }

    @Override
    public void close() {
        clear();
    }
}