package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class BlockSegment<T> extends Segment<Block> {

    private final Serializer<T> serializer;
    private final BlockFactory blockFactory;
    private final int blockSize;
    private final BiConsumer<Long, Block> blockWriteListener;
    protected Block block;
    private int blocks;

    public BlockSegment(File file,
                        StorageMode storageMode,
                        long dataLength,
                        IDataStream dataStream,
                        String magic,
                        WriteMode writeMode,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize) {
        this(file, storageMode, dataLength, dataStream, magic, writeMode, serializer, blockFactory, codec, blockSize, (p, b) -> {
        });
    }

    public BlockSegment(File file,
                        StorageMode storageMode,
                        long dataLength,
                        IDataStream dataStream,
                        String magic,
                        WriteMode writeMode,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize,
                        BiConsumer<Long, Block> blockWriteListener) {
        super(file, storageMode, dataLength, new BlockSerializer(codec, blockFactory), dataStream, magic, writeMode);
        this.serializer = serializer;
        this.blockFactory = blockFactory;
        this.blockSize = blockSize;
        this.blockWriteListener = blockWriteListener;
        this.block = blockFactory.create(blockSize);
    }

    public synchronized long add(T entry) {
        if (!hasSpaceAvailableForBlock()) {
            if (!block.isEmpty()) {
                throw new IllegalStateException("Block was not empty");
            }
            return Storage.EOF;
        }
        ByteBuffer bb = serializer.toBytes(entry);
        if (!block.add(bb)) {
            writeBlock();
            if (!hasSpaceAvailableForBlock()) {
                return Storage.EOF;
            }
            if (!block.add(bb)) {
                throw new IllegalStateException("Could not write to new block after flushing, block must ensure entry can be written or thrown an error");
            }
        }
        entries.incrementAndGet();
        return super.position();
    }

    private boolean hasSpaceAvailableForBlock() {
        long writePos = position();
        long logSize = logSize();
        return writePos + blockSize < logSize;
    }

    @Override
    protected void incrementEntry() {
        //do nothing
    }

    public synchronized void writeBlock() {
        if (block.isEmpty()) {
            return;
        }

        long blockPos = super.append(block);
        if (blockPos == Storage.EOF) {
            throw new IllegalStateException("Got EOF when writing non empty block");
        }

        this.blockWriteListener.accept(blockPos, block);
        this.block = blockFactory.create(blockSize);
        this.blocks++;
    }

    public SegmentIterator<T> entryIterator(Direction direction) {
        return new BlockIterator<>(serializer, super.iterator(direction), direction);
    }

    public SegmentIterator<T> entryIterator(long position, Direction direction) {
        return new BlockIterator<>(serializer, super.iterator(position, direction), direction);
    }

    public Stream<T> streamEntries(Direction direction) {
        return Iterators.closeableStream(entryIterator(direction));
    }

    public Stream<T> streamEntries(long position, Direction direction) {
        return Iterators.closeableStream(entryIterator(position, direction));
    }

    public int blocks() {
        return blocks;
    }

    @Override
    public Block get(long position) {
        if (position == this.position()) {
            return block;
        }
        return super.get(position);
    }

    @Override
    public synchronized void flush() {
        if (readOnly()) {
            return;
        }
        writeBlock();
        super.flush();
    }

    @Override
    public synchronized void roll(int level) {
        writeBlock();
        super.roll(level);
    }

    @Override
    protected long processEntries(List<RecordEntry<Block>> items) {
        long entryCount = 0;
        for (RecordEntry<Block> item : items) {
            entryCount += item.entry().entryCount();
        }
        blocks += items.size();
        return entryCount;
    }

    @Override
    public long uncompressedSize() {
        return blocks * blockSize; //approximation based on block size
    }

    public int blockSize() {
        return blockSize;
    }

    public static <T> SegmentFactory<T> factory(Codec codec, int blockSize) {
        return factory(codec, blockSize, VLenBlock.factory());
    }

    public static <T> SegmentFactory<T> factory(Codec codec, int blockSize, BlockFactory blockFactory) {
        return (file, storageMode, dataLength, serializer, reader, magic, type) -> {
            BlockSegment<T> delegate = new BlockSegment<>(file, storageMode, dataLength, reader, magic, type, serializer, blockFactory, codec, blockSize);
            return new BlockSegmentWrapper<>(delegate);
        };
    }

}
