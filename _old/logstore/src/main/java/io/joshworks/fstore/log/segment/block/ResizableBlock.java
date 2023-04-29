package io.joshworks.fstore.log.segment.block;


import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class ResizableBlock extends Block {

    public ResizableBlock(int initialSize) {
        super(initialSize);
    }

    /**
     * Always returns true as it expands when there's no space in the block.
     *
     * @return always true
     */
    @Override
    public boolean add(ByteBuffer entry) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        if (entry.remaining() == 0) {
            throw new IllegalArgumentException("Empty data is not allowed");
        }
        int entrySize = entry.remaining();
        int remaining = data.remaining();
        int entryWithHeaderSize = entrySize + entryHeaderSize();
        if (remaining < entryWithHeaderSize) {
            expand(entryWithHeaderSize + data.capacity());
        }

        data.putInt(entrySize);
        lengths.add(entrySize);
        positions.add(data.position());
        data.put(entry);
        return true;
    }

    private void expand(int newSize) {
        ByteBuffer copy = Buffers.allocate(newSize, data.isDirect());
        data.flip();
        copy.put(data);
        data = copy;
    }
}
