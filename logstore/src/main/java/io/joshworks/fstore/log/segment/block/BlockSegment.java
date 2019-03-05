package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;

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

    public BlockSegment(Storage storage,
                        IDataStream dataStream,
                        String magic,
                        Type type,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize) {
        this(storage, dataStream, magic, type, serializer, blockFactory, codec, blockSize, (p, b) -> {
        });
    }

    public BlockSegment(Storage storage,
                        IDataStream dataStream,
                        String magic,
                        Type type,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize,
                        BiConsumer<Long, Block> blockWriteListener) {
        super(storage, new BlockSerializer(codec, blockFactory), dataStream, magic, type);
        this.serializer = serializer;
        this.blockFactory = blockFactory;
        this.blockSize = blockSize;
        this.blockWriteListener = blockWriteListener;
        this.block = blockFactory.create(blockSize);
    }

    public long add(T entry) {
        if (!hasSpaceAvailableForBlock()) {
            if (!block.isEmpty()) {
                throw new IllegalStateException("Block was not empty");
            }
            return Storage.EOF;
        }
        ByteBuffer bb = serializer.toBytes(entry);
        if (!block.add(bb)) {
            writeBlock();
            return add(entry);
        }
        entries.incrementAndGet();
        return super.position();
    }

    private boolean hasSpaceAvailableForBlock() {
        long writePos = writePosition.get();
        long logSize = logSize();
        return writePos + blockSize <= logSize;
    }

    @Override
    protected void incrementEntry() {
        //do nothing
    }

    public void writeBlock() {
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

    public Block current() {
        return block;
    }

    @Override
    public void flush() {
        if (readOnly()) {
            return;
        }
        writeBlock();
        super.flush();
    }

    @Override
    public void roll(int level) {
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
}
