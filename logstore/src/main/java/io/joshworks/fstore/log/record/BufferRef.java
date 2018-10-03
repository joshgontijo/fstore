package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.Supplier;

public class BufferRef implements Supplier<ByteBuffer>, AutoCloseable {

    private ByteBuffer buffer;
    private final BufferPool pool;
    int[] markers ;
    int[] lengths;
    int i = 0;

    private BufferRef(ByteBuffer buffer, BufferPool pool) {
        this.buffer = buffer;
        this.pool = pool;
    }

    public static BufferRef ofEmpty() {
        return of(ByteBuffer.allocate(0), null);
    }

    public static BufferRef of(ByteBuffer buffer, BufferPool pool) {
        Objects.requireNonNull(buffer);
        return new BufferRef(buffer, pool);
    }

    public static BufferRef withMarker(ByteBuffer buffer, BufferPool pool, int[] markers, int[] lengths) {
        Objects.requireNonNull(buffer);
        BufferRef bufferRef = new BufferRef(buffer, pool);
        bufferRef.markers = markers;
        bufferRef.lengths = lengths;
        return bufferRef;
    }

    public ByteBuffer next() {
        buffer.limit(markers[i] + lengths[i]);
        buffer.position(markers[i]);
        i++;
        return buffer;
    }


//    public static ByteBuffer[] toBuffers(BufferRef... refs) {
//        ByteBuffer[] bufs = new ByteBuffer[refs.length];
//        for (int i = 0; i < refs.length; i++) {
//            bufs[i] = refs[i].get();
//        }
//        return bufs;
//    }
//
//    public static BufferRef[] toReferences(ByteBuffer... buffers) {
//        BufferRef[] refs = new BufferRef[buffers.length];
//        for (int i = 0; i < buffers.length; i++) {
//            refs[i] = of(buffers[i]);
//        }
//        return refs;
//    }
//
//    public static void clear(BufferRef[] refs) {
//        for (BufferRef ref : refs) {
//            ref.clear();
//        }
//    }

    @Override
    public ByteBuffer get() {
        ByteBuffer buf = this.buffer;
        return buf;
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