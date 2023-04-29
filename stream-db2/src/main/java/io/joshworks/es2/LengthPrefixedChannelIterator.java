package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.iterators.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class LengthPrefixedChannelIterator implements CloseableIterator<ByteBuffer> {

    private static final int LEN_LEN = Integer.BYTES;
    private final SegmentChannel channel;
    private ByteBuffer readBuffer = Buffers.allocate(512, false);
    private LengthPrefixedBufferIterator bufferIt;
    private long offset;

    public LengthPrefixedChannelIterator(SegmentChannel channel) {
        this(channel, 0);
    }

    public LengthPrefixedChannelIterator(SegmentChannel channel, long startPos) {
        this.channel = channel;
        this.offset = startPos;
        this.bufferIt = new LengthPrefixedBufferIterator(Buffers.EMPTY);
    }

    private void read() {
        if (offset >= channel.position()) {
            return;
        }

        readBuffer.clear();
        int read = channel.read(readBuffer, offset);
        if (read <= LEN_LEN) {
            return;
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
                return;
            }
        }

        bufferIt = new LengthPrefixedBufferIterator(readBuffer);
        assert bufferIt.hasNext();
    }

    public long position() {
        return offset;
    }

    @Override
    public boolean hasNext() {
        if (bufferIt.hasNext()) {
            return true;
        }
        read();
        return bufferIt.hasNext();
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        ByteBuffer slice = bufferIt.next();
        offset += slice.remaining();
        return slice;
    }
}
