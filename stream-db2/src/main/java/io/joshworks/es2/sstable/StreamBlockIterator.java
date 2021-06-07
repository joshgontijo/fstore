package io.joshworks.es2.sstable;

import io.joshworks.es2.LengthPrefixedBufferIterator;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class StreamBlockIterator implements CloseableIterator<ByteBuffer> {

    private final CloseableIterator<ByteBuffer> entryIterator;
    private ByteBuffer decompressed = Buffers.EMPTY;
    private CloseableIterator<ByteBuffer> blockIter = Iterators.empty();

    public StreamBlockIterator(CloseableIterator<ByteBuffer> entryIterator) {
        this.entryIterator = entryIterator;
    }

    private void decompress(ByteBuffer compressedBlock) {
        int decompressSize = StreamBlock.uncompressedSize(compressedBlock);
        if (decompressed.capacity() < decompressSize) {
            decompressed = Buffers.allocate(decompressSize, false);
        }
        decompressed.clear();
        decompressed.limit(decompressSize);
        StreamBlock.decompress(compressedBlock, decompressed);
        decompressed.flip();
        blockIter = new LengthPrefixedBufferIterator(decompressed);
    }

    @Override
    public boolean hasNext() {
        if (blockIter.hasNext()) {
            return true;
        }
        if (!entryIterator.hasNext()) {
            return false;
        }
        decompress(entryIterator.next());
        return blockIter.hasNext();
    }

    @Override
    public ByteBuffer next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return blockIter.next();
    }
}
