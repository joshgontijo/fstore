package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LengthPrefixedIterator implements Iterator<ByteBuffer> {

    private static final int LEN_LEN = Integer.BYTES;

    private ByteBuffer readBuffer = Buffers.allocate(512, false);
    private ByteBuffer recSlice = Buffers.EMPTY;
    private final SegmentChannel channel;
    private long offset;

    public LengthPrefixedIterator(SegmentChannel channel) {
        this.channel = channel;
        this.readBuffer.position(readBuffer.limit());
    }

    private ByteBuffer read() {
        if (offset >= channel.position()) {
            return Buffers.EMPTY;
        }

        readBuffer.clear();
        int read = channel.read(readBuffer, offset);
        if (read <= LEN_LEN) {
            return Buffers.EMPTY;
        }

        readBuffer.flip();
        var recSize = readBuffer.getInt(0); //size prefix expected to be Integer.BYTES
        assert recSize >= 0;
        if (recSize > readBuffer.remaining()) {
            readBuffer = Buffers.allocate(recSize, false);
            read = channel.read(readBuffer, offset);
            readBuffer.flip();
            if (read < recSize) {
                //possible broken record with a header
                return Buffers.EMPTY;
            }
        }

        ByteBuffer nextEntry = nextEntry();
        assert nextEntry.hasRemaining();
        return nextEntry;
    }

    private ByteBuffer nextEntry() {
        if (recSlice.hasRemaining()) { //required so it can be used from 'hasNext'
            return recSlice;
        }
        if (readBuffer.remaining() < LEN_LEN) {
            return Buffers.EMPTY;
        }
        int bpos = readBuffer.position();
        int recSize = readBuffer.getInt(bpos);
        if (readBuffer.remaining() < recSize) {
            return Buffers.EMPTY;
        }
        recSlice = readBuffer.slice(bpos, recSize);
        Buffers.offsetPosition(readBuffer, recSize);
        return recSlice;
    }

    public long position() {
        return offset;
    }

    @Override
    public boolean hasNext() {
        return recSlice.hasRemaining() || nextEntry().hasRemaining() || read().hasRemaining();
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        ByteBuffer slice = recSlice;
        offset += slice.remaining();
        recSlice = Buffers.EMPTY;
        return slice;
    }
}
