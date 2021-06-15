package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.index.filter.BloomFilter;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class BPTreeIndex {

    static final int BLOCK_SIZE = Memory.PAGE_SIZE;
    private static final int FOOTER_SIZE = Long.BYTES * 2;

    private final SegmentChannel channel;
    private final SegmentChannel.MappedReadRegion mf;
    private final Block root;
    private final long denseEntries;

    private final BloomFilter filter;
    private final ByteBuffer keyBuffer = Buffers.allocate(IndexKey.BYTES, false); //Bloom filter only

    private BPTreeIndex(SegmentChannel channel, SegmentChannel.MappedReadRegion mf, long denseEntries, BloomFilter filter) {
        this.channel = channel;
        this.mf = mf;
        this.denseEntries = denseEntries;
        this.filter = filter;
        this.root = readRoot();
    }

    public static BPTreeIndex open(File file) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }

            var channel = SegmentChannel.open(file);
            //read footer
            ByteBuffer footer = Buffers.allocate(FOOTER_SIZE, false);
            int read = channel.read(footer, channel.position() - FOOTER_SIZE);
            assert read == FOOTER_SIZE;
            footer.flip();
            var indexSize = footer.getLong();
            var denseEntries = footer.getLong();

            //filter
            int filterSize = (int) (channel.size() - indexSize - FOOTER_SIZE);
            ByteBuffer filterBuf = Buffers.allocate(filterSize, false);
            read = channel.read(filterBuf, indexSize);
            assert read == filterSize;
            var filter = BloomFilter.load(filterBuf.flip());

            var mappedRegion = channel.map(0, (int) indexSize);
            checkChannelSize(channel, indexSize);

            return new BPTreeIndex(channel, mappedRegion, denseEntries, filter);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    private static void checkChannelSize(SegmentChannel channel, long indexSize) {
        if (indexSize > Buffers.MAX_CAPACITY) {
            channel.close();
            throw new RuntimeIOException("Index too big");
        }
        if (indexSize % IndexEntry.BYTES != 0) {
            channel.close();
            throw new IllegalStateException("Invalid index file length: " + indexSize);
        }
    }

    public IndexEntry find(long stream, int version, IndexFunction fn) {
        Block block = root;
        while (true) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(stream, version, IndexFunction.FLOOR);
                if (i == -1) {
                    return null;
                }
                int blockIdx = block.blockIndex(i);
                block = loadBlock(blockIdx);
            } else { //leaf node
                int i = block.find(stream, version, fn);
                if (i == -1) {
                    return null;
                }
                return block.toIndexEntry(i);
            }
        }
    }

    private Block loadBlock(int idx) {
        if (idx < 0 || idx >= numBlocks()) {
            throw new IndexOutOfBoundsException(idx);
        }
        int offset = idx * BLOCK_SIZE;
        ByteBuffer readBuffer = mf.slice(offset, BLOCK_SIZE);
        return Block.from(readBuffer);
    }

    public int entries() {
        return root.entries();
    }

    public void delete() {
        mf.close();
        channel.close();
    }

    private Block readRoot() {
        return loadBlock(numBlocks() - 1);
    }

    public void close() {
        mf.close();
    }

    public int numBlocks() {
        return mf.position() / BLOCK_SIZE;
    }

    public int size() {
        return mf.capacity();
    }

    //actual index entries
    public int sparseEntries() {
        return mf.capacity() / IndexEntry.BYTES;
    }

    //dense entry count, used for filter or total count
    public long denseEntries() {
        return denseEntries;
    }

    private static class IndexWriter implements Closeable {

        private static final ThreadLocal<TreeBuffers> blocks = ThreadLocal.withInitial(TreeBuffers::new);
        private final SegmentChannel channel;
        private final BloomFilter filter;
        private final ByteBuffer buffer = Buffers.allocate(IndexKey.BYTES, false);
        private long entries;

        public IndexWriter(File file, int expectedEntries, double bpFpPercentage) {
            this.channel = SegmentChannel.create(file);
            this.filter = BloomFilter.create(expectedEntries, bpFpPercentage);
            for (Block block : blocks.get().nodeBlocks) {
                block.clear();
            }
        }

        public void stampEntry(long stream, int version) {
            filter.add(buffer
                    .clear()
                    .putLong(stream)
                    .putInt(version)
                    .flip());

            entries++;
        }

        public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
            Block node = blocks.get().getOrAllocate(0);
            if (!node.add(stream, version, recordSize, recordEntries, logPos)) {
                writeNode(node);
                node.add(stream, version, recordSize, recordEntries, logPos);
            }
        }

        public void writeNode(Block node) {
            int idx = node.writeTo(channel);

            //link node to the parent
            Block parent = blocks.get().getOrAllocate(node.level() + 1);
            if (!parent.addLink(node, idx)) {
                writeNode(parent);
                parent.addLink(node, idx);
            }
        }

        //write not without linking node to the parent, used only for root when completing segment
        private void writeNodeFinal(Block node) {
            node.writeTo(channel);
        }

        public void complete() {
            //flush remaining nodes
            List<Block> nodeBlocks = blocks.get().nodeBlocks;
            for (int i = 0; i < nodeBlocks.size() - 1; i++) {
                Block block = nodeBlocks.get(i);
                if (block.hasData()) {
                    writeNode(block);
                }
            }
            //flush root
            Block root = nodeBlocks.get(nodeBlocks.size() - 1);
            if (root.hasData()) {
                writeNodeFinal(root);
            }

            for (Block block : nodeBlocks) {
                assert !block.hasData();
                block.clear();
            }

            channel.truncate();
            channel.flush();

            assert channel.size() % BPTreeIndex.BLOCK_SIZE == 0 : "Unaligned index";
        }

        @Override
        public void close() {
            for (Block block : blocks.get().nodeBlocks) {
                block.clear();
            }
        }


        private static class TreeBuffers {
            private final List<Block> nodeBlocks = new ArrayList<>();

            private Block getOrAllocate(int level) {
                if (level >= nodeBlocks.size()) {
                    nodeBlocks.add(level, Block.create(Memory.PAGE_SIZE, level));
                }
                var block = nodeBlocks.get(level);
                assert block.level() == level;
                return block;
            }

        }

    }

}