package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.LogHeader;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.ByteBufferCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class BlockSegment<T> implements Log<T> {

    public static final long START = withBlockIndex(0, Segment.START);
    private static final Logger logger = LoggerFactory.getLogger(BlockSegment.class);

    private final int maxBlockSize;
    private final Log<ByteBuffer> delegate;
    private final Serializer<T> serializer;
    private final BlockFactory<T> factory;
    private final Codec codec;

    //TODO add block cache

    private Block<T> block;
    private long entries;

    public BlockSegment(Storage storage, Serializer<T> serializer, IDataStream dataStream, String magic, Type type, BlockFactory<T> factory, Codec codec, int maxBlockSize) {
        //Direct serializer must not be used in the delegate segment
        this.delegate = new Segment<>(storage, new ByteBufferCopy(), dataStream, magic, type);
        this.codec = codec;
        this.factory = factory;
        this.serializer = serializer;
        this.maxBlockSize = maxBlockSize;
        this.block = factory.create(serializer, maxBlockSize);
    }

    @Override
    public long append(T data) {
        int blockEntries = block.entryCount();
        long logPos = delegate.position();
        if (blockEntries >= LogAppender.MAX_BLOCK_VALUE) {
            writeBlock(); //must not return the block position
        }
        if (block.add(data)) {
            writeBlock();//must not return the block position
        }
        entries++;
        return withBlockIndex(blockEntries, logPos);
    }

    public static long withBlockIndex(long entryIdx, long position) {
        if (entryIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (entryIdx > LogAppender.MAX_BLOCK_VALUE) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + LogAppender.MAX_BLOCK_VALUE);
        }
        return (position << LogAppender.BLOCK_BITS) | entryIdx;
    }

    public static long blockPosition(long position) {
        return (position >>> LogAppender.BLOCK_BITS);
    }

    public static int entryIdx(long position) {
        long mask = (1L << LogAppender.BLOCK_BITS) - 1;
        return (int) (position & mask);
    }

    //returns the block position
    protected long writeBlock() {
        if (block.isEmpty()) {
            return position();
        }
        ByteBuffer blockData = block.pack(codec);
        long blockPos = delegate.append(blockData);
        this.block = factory.create(serializer, maxBlockSize);
        return withBlockIndex(0, blockPos);
    }

    protected Block<T> currentBlock() {
        return block;
    }

    @Override
    public void flush() {
        writeBlock();
        delegate.flush();
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
    public Marker marker() {
        return delegate.marker();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
    }

    @Override
    public long position() {
        return withBlockIndex(block.entryCount(), delegate.position());
    }

    @Override
    public T get(long position) {
        validateBlockPosition(position);
        long blockPos = blockPosition(position);
        int idx = entryIdx(position);
        if (idx < 0 || idx > LogAppender.MAX_BLOCK_VALUE) {
            throw new IllegalArgumentException("Invalid block entry index: " + idx);
        }

        ByteBuffer blockData = delegate.get(blockPos);
        if (blockData == null) {
            return null;
        }
        Block<T> loadedBlock = factory.load(serializer, codec, blockData);
        return loadedBlock.get(idx);
    }

    public Block<T> getBlock(long position) {
        long bPos = blockPosition(position);
        ByteBuffer data = delegate.get(bPos);
        if (data == null) {
            throw new IllegalStateException("No block on address " + position);
        }
        return factory.load(serializer, codec, data);
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        if (lastKnownPosition < START) {
            throw new IllegalStateException("Invalid lastKnownPosition: " + lastKnownPosition + ",value must be at least " + START);
        }
        long position = lastKnownPosition;
        int foundEntries = 0;
        long start = System.currentTimeMillis();
        try {
            logger.info("Restoring log state and checking consistency from position {}", lastKnownPosition);

            try (LogIterator<ByteBuffer> iterator = delegate.iterator(Direction.FORWARD)) {
                while (iterator.hasNext()) {
                    position = iterator.position();
                    ByteBuffer blockData = iterator.next();
                    Block<T> blockRead = factory.load(serializer, codec, blockData);
                    foundEntries += blockRead.entryCount();
                }
            }

        } catch (Exception e) {
            logger.warn("Found inconsistent entry on position {}, segment '{}': {}", position, name(), e.getMessage());
        }
        logger.info("Log state restored in {}ms, current position: {}, entries: {}", (System.currentTimeMillis() - start), position, foundEntries);
        if (position < LogHeader.BYTES) {
            throw new IllegalStateException("Initial log state position must be at least " + LogHeader.BYTES);
        }
        return new SegmentState(foundEntries, position);
    }

    @Override
    public void delete() {
        block = factory.create(serializer, maxBlockSize);
        delegate.delete();
    }

    @Override
    public void roll(int level, ByteBuffer footer) {
        delegate.roll(level, footer);
    }

    @Override
    public ByteBuffer readFooter() {
        return delegate.readFooter();
    }

    @Override
    public PollingSubscriber<T> poller(long position) {
        validateBlockPosition(position);
        return new BlockPoller<>(delegate, factory, serializer, codec, position);
    }

    @Override
    public PollingSubscriber<T> poller() {
        return new BlockPoller<>(delegate, factory, serializer, codec);
    }

    @Override
    public void roll(int level) {
        flush();
        delegate.roll(level);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public long entries() {
        return entries;
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public void close() throws IOException {
        flush();
        delegate.close();
    }

    @Override
    public LogIterator<T> iterator(Direction direction) {
        long position = Direction.FORWARD.equals(direction) ? START : withBlockIndex(0, delegate.position());
        return iterator(position, direction);
    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        validateBlockPosition(position);
        return new BlockIterator<>(delegate, factory, serializer, codec, position, direction);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private void validateBlockPosition(long position) {
        if(position < START) {
            throw new IllegalArgumentException("Block Position must be at least: " + START + ", got: " + position);
        }
        long bPos = blockPosition(position);
        long entryIdx = entryIdx(position);
        if(bPos < Segment.START || bPos > LogAppender.MAX_SEGMENT_ADDRESS) {
            throw new IllegalArgumentException("Invalid block position: " + bPos);
        }
        if(entryIdx < 0 || entryIdx > LogAppender.MAX_BLOCK_VALUE) {
            throw new IllegalArgumentException("Invalid entry index: " + entryIdx);

        }
    }

    private static class BlockIterator<T> implements LogIterator<T> {

        private final Direction direction;
        private final LogIterator<ByteBuffer> segmentIterator;
        private final BlockFactory<T> factory;
        private final Serializer<T> serializer;
        private final Codec codec;

        private final Queue<T> cached = new LinkedList<>();
        private int blockRead;
        private int blockSize;
        private long currentBlockPos;

        private BlockIterator(Log<ByteBuffer> delegate, BlockFactory<T> factory, Serializer<T> serializer, Codec codec, long position, Direction direction) {
            this.factory = factory;
            this.serializer = serializer;
            this.codec = codec;
            this.currentBlockPos = blockPosition(position);
            this.segmentIterator = delegate.iterator(currentBlockPos, direction);
            this.direction = direction;

            int entryIdx = entryIdx(position);
            if (entryIdx > 0) {
                if (Direction.BACKWARD.equals(direction)) {
                    //we need to read forward the first since blockPosition() returns the start of the block
                    try (LogIterator<ByteBuffer> fit = delegate.iterator(currentBlockPos, Direction.FORWARD)) {
                        ByteBuffer blockData = fit.next();
                        parseBlock(blockData);
                        int skip = cached.size() - entryIdx;
                        for (int i = 0; i < skip; i++) {
                            readCached();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                if (Direction.FORWARD.equals(direction)) {
                    readBlock();
                    for (int i = 0; i < entryIdx; i++) {
                        readCached();
                    }
                }
            }
        }

        private T readCached() {
            T polled = cached.poll();
            if (polled != null) {
                blockRead++;
            }
            return polled;
        }

        private void parseBlock(ByteBuffer blockData) {
            Block<T> block = factory.load(serializer, codec, blockData);
            List<T> entries = block.entries();
            if (Direction.BACKWARD.equals(direction)) {
                Collections.reverse(entries);
            }
            blockSize = entries.size();
            blockRead = 0;
            cached.addAll(entries);
        }

        private void readBlock() {
            currentBlockPos = segmentIterator.position();
            ByteBuffer blockData = segmentIterator.next();
            if (blockData == null) {
                throw new NoSuchElementException();
            }
            parseBlock(blockData);
        }

        @Override
        public long position() {
            if (Direction.FORWARD.equals(direction)) {
                if (blockRead == blockSize) {
                    return withBlockIndex(0, segmentIterator.position());
                }
                return withBlockIndex(blockRead, currentBlockPos);
            } else {
                int idx = blockSize - blockRead;
                if (blockRead == 0) {
                    return withBlockIndex(0, segmentIterator.position());
                }
                return withBlockIndex(idx, segmentIterator.position());
            }
        }

        @Override
        public boolean hasNext() {
            if (!cached.isEmpty()) {
                return true;
            }
            if (segmentIterator.hasNext()) {
                return true;
            }
            IOUtils.closeQuietly(segmentIterator);
            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (cached.isEmpty()) {
                readBlock();
            }

            T polled = readCached();
            if (polled == null) {
                throw new NoSuchElementException();
            }
            return polled;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(segmentIterator);
        }

    }

    private static final class BlockPoller<T> implements PollingSubscriber<T> {

        private final PollingSubscriber<ByteBuffer> segmentPoller;
        private final BlockFactory<T> factory;
        private final Serializer<T> serializer;
        private final Codec codec;
        private Queue<T> cachedBlockEntries = new LinkedList<>();
        private int blockRead = 0;

        public BlockPoller(Log<ByteBuffer> delegate, BlockFactory<T> factory, Serializer<T> serializer, Codec codec, long position) {
            int entryIdx = entryIdx(position);
            long logPos = blockPosition(position);
            this.serializer = serializer;
            this.codec = codec;
            this.segmentPoller = delegate.poller(logPos);
            this.factory = factory;
            if (entryIdx > 0) {
                for (int i = 0; i < entryIdx; i++) {
                    try {
                        segmentPoller.poll(); //skip
                    } catch (Exception e) {
                        IOUtils.closeQuietly(segmentPoller);
                        throw new RuntimeException(e);
                    }
                }
            }

        }

        private BlockPoller(Log<ByteBuffer> delegate, BlockFactory<T> factory, Serializer<T> serializer, Codec codec) {
            this.serializer = serializer;
            this.codec = codec;
            this.segmentPoller = delegate.poller();
            this.factory = factory;
        }

        @Override
        public synchronized T peek() throws InterruptedException {
            tryPoolFromDisk();
            return cachedBlockEntries.peek();
        }

        @Override
        public synchronized T poll() throws InterruptedException {
            tryPoolFromDisk();
            return cachedBlockEntries.poll();
        }

        private T tryReadCached() {
            T polled = cachedBlockEntries.poll();
            if (polled != null) {
                blockRead++;
            }
            return polled;
        }

        private void tryPoolFromDisk() throws InterruptedException {
            tryPoolFromDisk(-1, TimeUnit.MILLISECONDS);
        }

        private void tryPoolFromDisk(long limit, TimeUnit timeUnit) throws InterruptedException {
            ByteBuffer polled = segmentPoller.poll(limit, timeUnit);
            if (polled != null) {
                Block<T> blockData = factory.load(serializer, codec, polled);
                cachedBlockEntries.addAll(blockData.entries());
            }
            blockRead = 0;
        }

        @Override
        public synchronized T poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            T pooled = tryReadCached();
            if (pooled != null) {
                return pooled;
            }
            tryPoolFromDisk(limit, timeUnit);
            return tryReadCached();
        }

        @Override
        public synchronized T take() throws InterruptedException {
            T pooled = tryReadCached();
            if (pooled != null) {
                return pooled;
            }

            ByteBuffer data = segmentPoller.take();
            if (data != null) {
                Block<T> blockData = factory.load(serializer, codec, data);
                cachedBlockEntries.addAll(blockData.entries());
            }
            blockRead = 0;
            return cachedBlockEntries.poll();
        }

        @Override
        public synchronized boolean headOfLog() {
            return cachedBlockEntries.isEmpty() && segmentPoller.headOfLog();
        }

        @Override
        public synchronized boolean endOfLog() {
            return cachedBlockEntries.isEmpty() && segmentPoller.endOfLog();
        }

        @Override
        public long position() {
            return withBlockIndex(blockRead, segmentPoller.position());
        }

        @Override
        public synchronized void close() throws IOException {
            segmentPoller.close();
        }
    }

}
