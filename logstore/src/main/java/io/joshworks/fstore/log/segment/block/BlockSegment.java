package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.header.Type;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Stream;

public class BlockSegment<T> implements Log<T> {

    private final BlockSerializer<T> blockSerializer;
    private final Serializer<T> serializer;
    private final BlockFactory<T> blockFactory;
    private final int blockSize;
    private final Segment<Block<T>> delegate;

    private Block<T> block;

    public BlockSegment(Storage storage,
                        DataStream dataStream,
                        String magic,
                        Type type,
                        Serializer<T> serializer,
                        BlockFactory<T> blockFactory,
                        Codec codec,
                        int blockSize) {
        this.serializer = serializer;
        this.blockFactory = blockFactory;
        this.blockSize = blockSize;
        this.blockSerializer = new BlockSerializer<>(codec, blockFactory, serializer);
        this.delegate = new Segment<>(storage, blockSerializer, dataStream, magic, type);
        this.block = blockFactory.create(serializer, blockSize);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public Marker marker() {
        return delegate.marker();
    }

    @Override
    public T get(long position) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        return new BlockPoller<>(delegate.poller(position));
    }

    @Override
    public PollingSubscriber<T> poller() {
        return new BlockPoller<>(delegate.poller());
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
    }

    @Override
    public long append(T data) {
        long pos = delegate.position();
        if (block.add(data)) {
            writeBlock();
        }
        return pos;
    }

    public Block<T> getBlock(long position) {
        return delegate.get(position);
    }

    protected long writeBlock() {
        if (block.isEmpty()) {
            return position();
        }
        long blockPos = delegate.append(block);
        this.block = blockFactory.create(serializer, blockSize);
        return blockPos;
    }

    public LogIterator<Block<T>> blockIterator(Direction direction) {
        return delegate.iterator(direction);
    }

    public LogIterator<Block<T>> blockIterator(long position, Direction direction) {
        return delegate.iterator(position, direction);
    }

    public Stream<Block<T>> streamBlock(Direction direction) {
        return Iterators.closeableStream(blockIterator(direction));
    }

    public Stream<Block<T>> streamBlock(long position, Direction direction) {
        return Iterators.closeableStream(blockIterator(position, direction));
    }

    public PollingSubscriber<Block<T>> blockPoller() {
        return delegate.poller();
    }

    public PollingSubscriber<Block<T>> blockPoller(long position) {
        return delegate.poller(position);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Stream<T> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    @Override
    public LogIterator<T> iterator(Direction direction) {
        return new BlockIterator<>(delegate.iterator(direction), direction);
    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        return new BlockIterator<>(delegate.iterator(position, direction), direction);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void flush() {
        if(readOnly()) {
            return;
        }
        writeBlock();
        delegate.flush();
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
        writeBlock();
        delegate.roll(level);
    }

    @Override
    public void roll(int level, ByteBuffer footer) {
        flush();
        delegate.roll(level, footer);
    }

    @Override
    public ByteBuffer readFooter() {
        return delegate.readFooter();
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
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
    public Type type() {
        return delegate.type();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long actualSize() {
        return delegate.actualSize();
    }

}
