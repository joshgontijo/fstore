package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.iterators.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class LengthPrefixedBufferIterator implements CloseableIterator<ByteBuffer> {

    private static final int LEN_LEN = Integer.BYTES;

    private final ByteBuffer data;
    private int offset;

    public LengthPrefixedBufferIterator(ByteBuffer data) {
        this.data = data;
        this.offset = 0;
    }

    public long read() {
        return offset;
    }

    @Override
    public boolean hasNext() {
        int remaining = data.remaining();
        //assumes recLen value includes the len field itself
        return remaining >= LEN_LEN &&
                data.getInt(data.position()) <= remaining;
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        var rpos = data.position();
        var recLen = data.getInt(rpos); //returns LEN_LEN field as well
        var slice = data.slice(rpos, recLen);
        Buffers.offsetPosition(data, recLen); //assumes recLen value includes the len field itself
        this.offset += recLen;
        return slice;
    }
}
