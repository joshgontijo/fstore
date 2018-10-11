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
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class BlockSegment<T> implements Log<T> {

    private static final Logger logger = LoggerFactory.getLogger(BlockSegment.class);

    private final int maxBlockSize;
    private final Log<ByteBuffer> delegate;
    private final Serializer<T> serializer;
    private final BlockFactory<T> factory;
    private final Codec codec;

    //TODO add block cached

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
        if (blockEntries >= LogAppender.MAX_BLOCK_ENTRIES) {
            writeBlock();
        }
        if (block.add(data)) {
            writeBlock();
        }
        entries++;
        return withBlockIndex(blockEntries, delegate.position());
    }

    private long withBlockIndex(long entryIdx, long position) {
        if (entryIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (entryIdx > LogAppender.MAX_BLOCK_ENTRIES) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + LogAppender.MAX_BLOCK_ENTRIES);
        }
        return (entryIdx << LogAppender.BLOCK_BITS) | position;
    }

    private int entryIdx(long position) {
        return (int) (position >>> LogAppender.BLOCK_BITS);
    }

    private long blockPosition(long position) {
        long mask = (1L << LogAppender.BLOCK_BITS) - 1;
        return (position & mask);
    }


    private void writeBlock() {
        if (block.isEmpty()) {
            return;
        }
        ByteBuffer blockData = block.pack(codec);
        delegate.append(blockData);
        this.block = factory.create(serializer, maxBlockSize);
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
        long blockPos = blockPosition(position);
        int idx = entryIdx(position);
        if (idx < 0) {
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
        ByteBuffer data = delegate.get(position);
        if (data == null) {
            throw new IllegalStateException("Block not data on address " + position);
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
        return new BlockPoller(delegate.poller(position));
    }

    @Override
    public PollingSubscriber<T> poller() {
        return new BlockPoller(delegate.poller());
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
        long position = Direction.FORWARD.equals(direction) ? Log.START : delegate.position();
        return iterator(position, direction);
    }

    @Override
    public LogIterator<T> iterator(long position, Direction direction) {
        return new BlockIterator(position, direction);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private class BlockIterator implements LogIterator<T> {

        private final Direction direction;
        private final LogIterator<ByteBuffer> segmentIterator;

        private Block<T> block;
        private int entryIdx;
        private long currentBlockPos;

        private BlockIterator(long position, Direction direction) {
            this.currentBlockPos = blockPosition(position);
            this.entryIdx = entryIdx(position);
            this.segmentIterator = delegate.iterator(currentBlockPos, direction);
            this.direction = direction;
            if (Direction.BACKWARD.equals(direction) && entryIdx > 0) {
                //we need to read forward the first since blockPosition() returns the start of the block
                try (LogIterator<ByteBuffer> fit = delegate.iterator(currentBlockPos, Direction.FORWARD)) {
                    ByteBuffer blockData = fit.next();
                    block = factory.load(serializer, codec, blockData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void readBlock() {
            ByteBuffer blockData = segmentIterator.next();
            if (blockData == null) {
                throw new NoSuchElementException();
            }
            block = factory.load(serializer, codec, blockData);
            if (Direction.BACKWARD.equals(direction)) {
                currentBlockPos = segmentIterator.position();
                entryIdx = block.entryCount() - 1;
            }

        }

        @Override
        public long position() {
            return withBlockIndex(entryIdx, currentBlockPos);
        }

        @Override
        public boolean hasNext() {
            if (segmentIterator.hasNext()) {
                return true;
            }
            IOUtils.closeQuietly(segmentIterator);
            return hasEntriesOnBlock(entryIdx);
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (block == null) {
                readBlock();
            }
            entryIdx = Direction.FORWARD.equals(direction) ? entryIdx : entryIdx - 1;
            T polled = block.get(entryIdx);
            if (polled == null) {
                readBlock();
                entryIdx = Direction.FORWARD.equals(direction) ? entryIdx + 1 : entryIdx - 1;
                polled = block.get(entryIdx);
            }

            if(Direction.BACKWARD.equals(direction) && entryIdx == 0) {
                block = null;
                currentBlockPos = segmentIterator.position();
            }

            int nextIdx = Direction.FORWARD.equals(direction) ? entryIdx + 1 : entryIdx - 1;
            if (!hasEntriesOnBlock(nextIdx)) {
                entryIdx = 0;
                block = null;
                currentBlockPos = segmentIterator.position();
            }
            return polled;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(segmentIterator);
        }

        private boolean hasEntriesOnBlock(int idx) {
            return block != null && idx >= 0 && idx < block.entryCount();
        }
    }

    private final class BlockPoller implements PollingSubscriber<T> {

        private final PollingSubscriber<ByteBuffer> segmentPoller;
        private Queue<T> cachedBlockEntries = new LinkedList<>();
        private int blockRead = 0;

        private BlockPoller(PollingSubscriber<ByteBuffer> segmentPoller) {
            this.segmentPoller = segmentPoller;
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
