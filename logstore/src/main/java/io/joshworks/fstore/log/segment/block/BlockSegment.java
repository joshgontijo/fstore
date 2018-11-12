package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.LogPoller;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class BlockSegment<T> extends Segment<Block<T>> {

    private final Serializer<T> serializer;
    private final BlockFactory<T> blockFactory;
    private final int blockSize;
    private final BiConsumer<Long, Block<T>> blockWriteListener;
    protected Block<T> block;

    public BlockSegment(Storage storage,
                        IDataStream dataStream,
                        String magic,
                        Type type,
                        Serializer<T> serializer,
                        BlockFactory<T> blockFactory,
                        Codec codec,
                        int blockSize) {
        this(storage, dataStream, magic, type, serializer, blockFactory, codec, blockSize, (p,b) ->{});
    }

    public BlockSegment(Storage storage,
                        IDataStream dataStream,
                        String magic,
                        Type type,
                        Serializer<T> serializer,
                        BlockFactory<T> blockFactory,
                        Codec codec,
                        int blockSize,
                        BiConsumer<Long, Block<T>> blockWriteListener) {
        super(storage, new BlockSerializer<>(codec, blockFactory, serializer), dataStream, magic, type);
        this.serializer = serializer;
        this.blockFactory = blockFactory;
        this.blockSize = blockSize;
        this.blockWriteListener = blockWriteListener;
        this.block = blockFactory.create(serializer, blockSize);
    }

    public long add(T entry) {
        if (block.add(entry)) {
            entries.incrementAndGet();
            return writeBlock();
        }
        entries.incrementAndGet();
        return super.position();
    }

    @Override
    protected void incrementEntry() {
        //do nothing
    }

    public long writeBlock() {
        if (block.isEmpty()) {
            return position();
        }
        long blockPos = super.append(block);
        blockWriteListener.accept(blockPos, block);
        this.block = blockFactory.create(serializer, blockSize);
        return blockPos;
    }

    public LogIterator<T> entryIterator(Direction direction) {
        return new BlockIterator<>(super.iterator(direction), direction);
    }

    public LogIterator<T> entryIterator(long position, Direction direction) {
        return new BlockIterator<>(super.iterator(position, direction), direction);
    }

    public Stream<T> streamEntries(Direction direction) {
        return Iterators.closeableStream(entryIterator(direction));
    }

    public Stream<T> streamEntries(long position, Direction direction) {
        return Iterators.closeableStream(entryIterator(position, direction));
    }

    public LogPoller<T> entryPoller() {
        return new BlockPoller<>(super.poller());
    }

    public LogPoller<T> entryPoller(long position) {
        return new BlockPoller<>(super.poller(position));
    }

    @Override
    public Block<T> get(long position) {
        if(position == this.position()) {
            return block;
        }
        return super.get(position);
    }

    @Override
    public void flush() {
        if(readOnly()) {
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
    protected long processEntries(List<Block<T>> items) {
        long entryCount = 0;
        for (Block<T> item : items) {
            entryCount += item.entryCount();
        }
        return entryCount;
    }
}
