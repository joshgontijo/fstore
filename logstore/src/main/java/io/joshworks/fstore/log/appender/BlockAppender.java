package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Stream;

public class BlockAppender<T, B extends Block<T>, L extends Log<B>> implements IBlockAppender<T, B, L> {

    private final LogAppender<B, L> delegate;
    private final BlockFactory<T, B> factory;
    private final int blockSize;
    private final Serializer<T> serializer;

    private B block;

    public BlockAppender(Config<T, L> config, BlockFactory<T, B> blockFactory, Codec codec, int blockSize) {
        BlockSerializer<T, B> blockSerializer = new BlockSerializer<>(codec, blockFactory, config.serializer);
        this.factory = blockFactory;
        this.blockSize = blockSize;
        this.serializer = config.serializer;
        this.block = factory.create(config.serializer, blockSize);
        this.delegate = LogAppender.builder(config.directory, blockSerializer).open();
    }

    private static long withBlockIndex(long entryIdx, long position) {
        if (entryIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (entryIdx > LogAppender.MAX_BLOCK_VALUE) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + LogAppender.MAX_BLOCK_VALUE);
        }
        return (position << LogAppender.BLOCK_BITS) | entryIdx;
    }

    private static long blockPosition(long position) {
        return  (position >>> LogAppender.BLOCK_BITS);
    }

    private static int entryIdx(long position) {
        long mask = (1L << LogAppender.BLOCK_BITS) - 1;
        return (int) (position & mask);
    }

    //returns the block position
    protected long writeBlock() {
        if (block.isEmpty()) {
            return position();
        }
        long blockPos = delegate.append(block);
        this.block = factory.create(serializer, blockSize);
        return withBlockIndex(0, blockPos);
    }


    @Override
    public void roll() {

    }

    @Override
    public void compact() {

    }

    @Override
    public long append(B data) {
        return 0;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public LogIterator<B> iterator(Direction direction) {
        return null;
    }

    @Override
    public Stream<B> stream(Direction direction) {
        return null;
    }

    @Override
    public LogIterator<B> iterator(long position, Direction direction) {
        return null;
    }

    @Override
    public PollingSubscriber<B> poller() {
        return null;
    }

    @Override
    public PollingSubscriber<B> poller(long position) {
        return null;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public B get(long position) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public LogIterator<L> segments(Direction direction) {
        return null;
    }

    @Override
    public Stream<L> streamSegments(Direction direction) {
        return null;
    }

    @Override
    public long size(int level) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }

    @Override
    public List<String> segmentsNames() {
        return null;
    }

    @Override
    public long entries() {
        return 0;
    }

    @Override
    public String currentSegment() {
        return null;
    }

    @Override
    public int depth() {
        return 0;
    }

    @Override
    public Path directory() {
        return null;
    }

    @Override
    public LogIterator<T> entryIterator() {
        return null;
    }


    private static class BlockSerializer<T, B extends Block<T>> implements Serializer<B> {

        private final Codec codec;
        private final BlockFactory<T, B> factory;
        private final Serializer<T> serializer;

        private BlockSerializer(Codec codec, BlockFactory<T, B> factory, Serializer<T> serializer) {
            this.codec = codec;
            this.factory = factory;
            this.serializer = serializer;
        }

        @Override
        public ByteBuffer toBytes(B data) {
            return data.pack(codec);
        }

        @Override
        public void writeTo(B  data, ByteBuffer dest) {
            //do nothing
        }

        @Override
        public B  fromBytes(ByteBuffer buffer) {
            return factory.load(serializer, codec, buffer);
        }
    }

    private class BlockIterator implements LogIterator<T> {

        private final LogIterator<Block<T>> it;
        private final Queue<T> queue = new LinkedList<>();

        private BlockIterator(LogIterator<Block<T>> it) {
            this.it = it;
        }

        @Override
        public long position() {
            return it.position();
        }

        @Override
        public void close() throws IOException {
            it.close();
        }

        @Override
        public boolean hasNext() {
            return queue.isEmpty() && !it.hasNext();
        }

        @Override
        public T next() {
            T next = queue.poll();
            if (next == null) {
                Block<T> loaded = it.next();
                if (loaded == null) {
                    throw new NoSuchElementException();
                }
                this.queue.addAll(loaded.entries());
            }
            T poll = queue.poll();
            if (poll == null) {
                throw new NoSuchElementException();
            }
            return poll;
        }
    }

}
