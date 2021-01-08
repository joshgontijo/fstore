package io.joshworks.es2.sstable;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

class LengthPrefixedDataIterator implements Iterator<ByteBuffer> {

    private ByteBuffer readBuffer = Buffers.allocate(512, false);
    private ByteBuffer recSlice = Buffers.EMPTY;
    private final SegmentChannel channel;
    private long offset;

    LengthPrefixedDataIterator(SegmentChannel channel, int startOffset) {
        this.channel = channel;
        this.offset = startOffset;
    }

    private ByteBuffer read() {
        int read = channel.read(readBuffer, offset);
        if (read <= 0) {
            return Buffers.EMPTY;
        }
        readBuffer.flip();
        int recSize = readBuffer.getInt(0); //size prefix expected to be Integer.BYTES
        if (recSize > readBuffer.remaining()) {
            readBuffer = Buffers.allocate(recSize, false);
            return read();
        }

        recSlice = readBuffer.slice(0, recSize);
        Buffers.offsetPosition(readBuffer, recSize);
        readBuffer.flip();
        return recSlice;
    }

    public ByteBuffer peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return recSlice;
    }

    @Override
    public boolean hasNext() {
        return recSlice.hasRemaining() || read().hasRemaining();
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        offset += recSlice.remaining();
        return recSlice;
    }
}
