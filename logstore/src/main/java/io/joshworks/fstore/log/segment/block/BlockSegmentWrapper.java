package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;

class BlockSegmentWrapper<T> implements Log<T> {

    private final BlockSegment<T> delegate;

    public BlockSegmentWrapper(BlockSegment<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long logSize() {
        return delegate.logSize();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        return delegate.entryIterator(direction);
    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        return delegate.entryIterator(position, direction);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public T get(long position) {
        throw new UnsupportedOperationException("Operation not allowed in block segments");
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return delegate.rebuildState(lastKnownPosition);
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public void roll(int level) {
        delegate.roll(level);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public boolean closed() {
        return delegate.closed();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public void truncate() {
        delegate.truncate();
    }

    @Override
    public long uncompressedSize() {
        return delegate.uncompressedSize();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public long append(T data) {
        return delegate.add(data);
    }

    @Override
    public void flush() {
        delegate.flush();
    }
}
