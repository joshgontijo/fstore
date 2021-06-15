package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class BlockIterator<T> implements SegmentIterator<T> {

    private final Serializer<T> serializer;
    private final SegmentIterator<Block> delegate;
    private final Direction direction;
    private final Queue<T> cached = new LinkedList<>();

    public BlockIterator(Serializer<T> serializer, SegmentIterator<Block> delegate, Direction direction) {
        this.serializer = serializer;
        this.delegate = delegate;
        this.direction = direction;
    }

    private void readNextBlock() {
        if (!delegate.hasNext()) {
            IOUtils.closeQuietly(this);
            return;
        }
        Block block = delegate.next();
        List<T> entries = block.deserialize(serializer);
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(entries);
        }
        cached.addAll(entries);
    }

    @Override
    public boolean hasNext() {
        if (!cached.isEmpty()) {
            return true;
        }
        if (delegate.hasNext()) {
            return true;
        }
        IOUtils.closeQuietly(this);
        return false;
    }

    @Override
    public T next() {
        if (cached.isEmpty()) {
            readNextBlock();
        }
        T found = cached.poll();
        if (found == null && !delegate.hasNext()) {
            IOUtils.closeQuietly(delegate);
            throw new NoSuchElementException();
        }
        return found;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        cached.clear();
        delegate.close();
    }

    @Override
    public boolean endOfLog() {
        return cached.isEmpty() && delegate.endOfLog();
    }
}
