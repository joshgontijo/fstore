package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BlockIterator<T> implements LogIterator<T> {

    private final LogIterator<Block<T>> delegate;
    private final Direction direction;
    private final Queue<T> cached = new LinkedList<>();

    public BlockIterator(LogIterator<Block<T>> delegate, Direction direction) {
        this.delegate = delegate;
        this.direction = direction;
    }

    private void readNextBlock() {
        Block<T> block = delegate.next();
        List<T> entries = block.entries();
        if (Direction.BACKWARD.equals(direction)) {
            Collections.reverse(entries);
        }
        cached.addAll(entries);
    }

    @Override
    public boolean hasNext() {
        return !cached.isEmpty() || delegate.hasNext();
    }

    @Override
    public T next() {
        if (cached.isEmpty()) {
            readNextBlock();
        }
        T found = cached.poll();
        if(found == null && !delegate.hasNext()) {
            IOUtils.closeQuietly(delegate);
        }
        return found;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        cached.clear();
        delegate.close();
    }

}
