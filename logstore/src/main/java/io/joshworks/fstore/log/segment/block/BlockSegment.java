package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BlockSegment<T> implements Log<T> {

    private static final String BLOCK_INFO_FOOTER_ITEM = "BLOCK_INFO";

    private final Serializer<T> serializer;
    private final BiConsumer<Long, Block> blockWriteListener;
    private final BiConsumer<Long, Block> onBlockLoaded;
    private final BufferPool bufferPool;
    private Consumer<FooterWriter> footerWriter;
    final Block writeBlock;

    private BlockSegmentInfo info;

    private final Segment<Block> delegate;
    private FooterReader footerReader;
    private final KryoStoreSerializer<BlockSegmentInfo> infoSerializer = KryoStoreSerializer.of(BlockSegmentInfo.class);

    public BlockSegment(File file,
                        StorageMode storageMode,
                        long dataLength,
                        BufferPool bufferPool,
                        WriteMode writeMode,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize,
                        double checksumProb,
                        int readPageSize) {
        this(
                file,
                storageMode,
                dataLength,
                bufferPool,
                writeMode,
                serializer,
                blockFactory,
                codec,
                blockSize,
                checksumProb,
                readPageSize,
                (p, b) -> {
                },
                (p, b) -> {
                },
                fWriter -> {
                });
    }

    public BlockSegment(File file,
                        StorageMode storageMode,
                        long dataLength,
                        BufferPool bufferPool,
                        WriteMode writeMode,
                        Serializer<T> serializer,
                        BlockFactory blockFactory,
                        Codec codec,
                        int blockSize,
                        double checksumProb,
                        int readPageSize,
                        BiConsumer<Long, Block> blockWriteListener,
                        BiConsumer<Long, Block> onBlockLoaded,
                        Consumer<FooterWriter> footerWriter) {

        this.serializer = serializer;
        this.bufferPool = bufferPool;
        this.info = new BlockSegmentInfo(blockSize);
        this.blockWriteListener = blockWriteListener;
        this.onBlockLoaded = onBlockLoaded;
        this.footerWriter = footerWriter;
        this.writeBlock = blockFactory.create(blockSize);
        this.delegate = new Segment<>(
                file,
                storageMode,
                dataLength,
                new BlockSerializer(codec, blockFactory),
                bufferPool,
                writeMode,
                checksumProb,
                readPageSize,
                this::processEntries,
                this::writeFooter);

        this.footerReader = delegate.footerReader();
        if (delegate.readOnly()) {
            this.info = footerReader.read(BLOCK_INFO_FOOTER_ITEM, infoSerializer);
        }
    }

    private void writeFooter(FooterWriter writer) {
        writer.write(BLOCK_INFO_FOOTER_ITEM, info, infoSerializer);
        footerWriter.accept(writer);
    }

    //When data is added to a block, it must ensure there will be space for the block in the segment
    @Override
    public long append(T entry) {
        //no more data available and the block is empty
        //do not add to the block, and return EOF, this allows that only one block gets inserted
        //when the the writePosition is greater than the maxDataSize
        if (!hasSpaceAvailableForBlock()) {
            if (!writeBlock.isEmpty()) {
                throw new IllegalStateException("No space remaining for block while is not empty");
            }
            return Storage.EOF;
        }
        if (!writeBlock.add(entry, serializer, bufferPool)) {
            writeBlock();
            if (!hasSpaceAvailableForBlock()) {
                return Storage.EOF;
            }
            if (!writeBlock.add(entry, serializer, bufferPool)) {
                throw new IllegalStateException("Could not write to new block after flushing, block must ensure entry can be written or thrown an error");
            }
        }
        info.addEntryCount(1);
        return delegate.position();
    }

    private boolean hasSpaceAvailableForBlock() {
        return delegate.remaining() >= info.blockSize();
    }

    private synchronized void writeBlock() {
        if (writeBlock.isEmpty()) {
            return;
        }

        info.addUncompressedSize(writeBlock.uncompressedSize());
        long blockPos = delegate.append(writeBlock);
        if (blockPos == Storage.EOF) {
            throw new IllegalStateException("Got EOF when writing non empty block");
        }

        this.blockWriteListener.accept(blockPos, writeBlock);
        writeBlock.clear();
    }

    public SegmentIterator<Block> blockIterator(Direction direction) {
        return delegate.iterator(direction);
    }

    public SegmentIterator<Block> blockIterator(long position, Direction direction) {
        return delegate.iterator(position, direction);
    }

    //actual entries present in all blovks
    public long totalEntries() {
        return info.entries();
    }

    public FooterReader footerReader() {
        return footerReader;
    }

    public long blocks() {
        return delegate.entries();
    }

    public Block getBlock(long blockPos) {
        return delegate.get(blockPos);
    }

    public List<T> readBlockEntries(long blockPos) {
        Block block = delegate.get(blockPos);
        if (block == null) {
            return Collections.emptyList();
        }
        return block.deserialize(serializer);
    }

    @Override
    public long physicalSize() {
        return delegate.physicalSize();
    }

    @Override
    public long logicalSize() {
        return delegate.logicalSize();
    }

    @Override
    public long dataSize() {
        return delegate.dataSize();
    }

    @Override
    public long actualDataSize() {
        return delegate.actualDataSize();
    }

    @Override
    public long uncompressedDataSize() {
        return info.uncompressedSize();
    }

    @Override
    public long headerSize() {
        return delegate.headerSize();
    }

    @Override
    public long footerSize() {
        return delegate.footerSize();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<T> iterator(long position, Direction direction) {
        return new BlockIterator<>(serializer, delegate.iterator(direction), direction);
    }

    @Override
    public SegmentIterator<T> iterator(Direction direction) {
        return new BlockIterator<>(serializer, delegate.iterator(direction), direction);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public T get(long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remaining() {
        return delegate.remaining() - writeBlock.uncompressedSize();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public synchronized void flush() {
        if (readOnly()) {
            return;
        }
        writeBlock();
        delegate.flush();
    }

    @Override
    public synchronized void roll(int level, boolean trim) {
        writeBlock();
        delegate.roll(level, trim);
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
        return info.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    private int processEntries(List<RecordEntry<Block>> items) {
        int entryCount = 0;
        for (RecordEntry<Block> recordEntry : items) {
            Block block = recordEntry.entry();
            info.addUncompressedSize(block.uncompressedSize());
            info.addCompressedSize(recordEntry.dataSize());
            entryCount += block.entryCount();
            onBlockLoaded.accept(recordEntry.position(), block);
        }
        info.addEntryCount(entryCount);
        return entryCount;
    }

    @Override
    public long uncompressedSize() {
        return info.uncompressedSize();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    @Override
    public Metrics metrics() {
        Metrics metrics = delegate.metrics();
        metrics.set("blockSize", info.blockSize());
        metrics.set("entries", info.entries());
        metrics.set("uncompressedSize", info.uncompressedSize());
        metrics.set("compressedSize", info.compressedSize());
        metrics.set("compressedSize", info.compressedSize());
        return metrics;
    }

    @Override
    public void close() {
        flush();
        delegate.close();
    }

    @Override
    public String toString() {
        return "BlockSegment{" + "serializer=" + serializer.getClass().getSimpleName() +
                ", bufferPool=" + bufferPool.getClass().getSimpleName() +
                ", info=" + info +
                ", delegate=" + delegate +
                '}';
    }

    public int blockSize() {
        return info.blockSize();
    }


    private static BiConsumer<Long, Block> NO_BLOCKWRITELISTENER = (a, b) -> {
    };
    private static BiConsumer<Long, Block> NO_ONBLOCKLOADED = (l, block) -> {
    };
    private static Consumer<FooterWriter> NO_FOOTERWRITER = fWriter -> {
    };

    public static <T> SegmentFactory<T> factory(Codec codec, int blockSize) {
        return factory(codec, blockSize, Block.vlenBlock(), NO_BLOCKWRITELISTENER, NO_ONBLOCKLOADED, NO_FOOTERWRITER);
    }

    public static <T> SegmentFactory<T> factory(Codec codec, int blockSize, BlockFactory blockFactory) {
        return factory(codec, blockSize, blockFactory, NO_BLOCKWRITELISTENER, NO_ONBLOCKLOADED, NO_FOOTERWRITER);
    }

    public static <T> SegmentFactory<T> factory(Codec codec,
                                                int blockSize,
                                                BiConsumer<Long, Block> blockWriteListener,
                                                BiConsumer<Long, Block> onBlockLoaded,
                                                Consumer<FooterWriter> footerWriter) {
        return factory(codec, blockSize, Block.vlenBlock(), blockWriteListener, onBlockLoaded, footerWriter);
    }

    public static <T> SegmentFactory<T> factory(Codec codec,
                                                int blockSize,
                                                BlockFactory blockFactory,
                                                BiConsumer<Long, Block> blockWriteListener,
                                                BiConsumer<Long, Block> onBlockLoaded,
                                                Consumer<FooterWriter> footerWriter) {
        return (file, storageMode, dataLength, serializer, bufferPool, writeMode, checksumProb, readPageSize) -> new BlockSegment<>(file, storageMode, dataLength, bufferPool, writeMode, serializer, blockFactory, codec, blockSize, checksumProb, readPageSize, blockWriteListener, onBlockLoaded, footerWriter);
    }

}
