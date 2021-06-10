package io.joshworks.es2.index;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.mmap.MappedFile;
import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;


public class BPTreeIndexSegment {

    static final int BLOCK_SIZE = Memory.PAGE_SIZE;

    private final MappedFile mf;
    private final Block root;

    public static BPTreeIndexSegment open(File file) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }
            MappedFile mf = MappedFile.open(file, FileChannel.MapMode.READ_ONLY);
            long fileSize = mf.capacity();
            if (fileSize % BLOCK_SIZE != 0) {
                throw new IllegalStateException("Invalid index file length: " + fileSize);
            }

            return new BPTreeIndexSegment(mf);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    private BPTreeIndexSegment(MappedFile mf) {
        this.mf = mf;
        this.root = readRoot();
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
        mf.delete();
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


    private static class IndexWriter implements Closeable {

        private static final ThreadLocal<TreeBuffers> buffer = ThreadLocal.withInitial(TreeBuffers::new);
        private final SegmentChannel channel;

        public IndexWriter(SegmentChannel channel) {
            this.channel = channel;
        }

        public void add(long stream, int version, int recordSize, int recordEntries, long logPos) {
            Block node = buffer.get().getOrAllocate(0);
            if (!node.add(stream, version, recordSize, recordEntries, logPos)) {
                writeNode(node);
                node.add(stream, version, recordSize, recordEntries, logPos);
            }
        }

        public void writeNode(Block node) {
            int idx = node.writeTo(channel);

            //link node to the parent
            Block parent = buffer.get().getOrAllocate(node.level() + 1);
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
            List<Block> nodeBlocks = buffer.get().nodeBlocks;
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

            assert channel.size() % BPTreeIndexSegment.BLOCK_SIZE == 0 : "Unaligned index";
        }

        @Override
        public void close() {
            for (Block block : buffer.get().nodeBlocks) {
                block.clear();
            }
        }


        private static class TreeBuffers {
            private final List<Block> nodeBlocks = new ArrayList<>();

            private Block getOrAllocate(int level) {
                if (level >= nodeBlocks.size()) {
                    nodeBlocks.add(level, Block.create(Memory.PAGE_SIZE, level));
                }
                Block block = nodeBlocks.get(level);
                assert block.level() == level;
                return block;
            }

        }

    }

}