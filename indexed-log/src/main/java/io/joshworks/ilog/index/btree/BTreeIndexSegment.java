package io.joshworks.ilog.index.btree;

import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;


public class BTreeIndexSegment extends Index {

    private final int blockSize;
    //read path
    private Block root;

    //write path
    private final List<ByteBuffer> nodeBlocks = new ArrayList<>();

    public BTreeIndexSegment(File file, long maxEntries, RowKey rowKey, int blockSize) {
        super(file, maxEntries, rowKey);
        this.blockSize = blockSize;
        if (readOnly.get()) {
            this.root = readRoot();
        }
    }

    @Override
    public void write(ByteBuffer rec, long recordPos) {
        assert !isFull() : "Index is full";

        Block node = getOrAllocate(0);
        if (!node.add(rec, recordPos)) {
            writeNode(node);
            node.add(rec, recordPos);
        }
    }

    public void writeNode(Block node) {
        int idx = node.writeTo(mf);

        //link node to the parent
        Block parent = getOrAllocate(node.level() + 1);
        if (!parent.addLink(node, idx)) {
            writeNode(parent);
            parent.addLink(node, idx);
        }
    }

    //write not without linking node to the parent, used only for root when completig segment
    public void writeNodeFinal(Block node) {
        node.writeTo(mf);
    }


    private Block getOrAllocate(int level) {
        if (level >= nodeBlocks.size()) {
            nodeBlocks.add(level, new Block(rowKey, blockSize, level));
        }
        return nodeBlocks.get(level);
    }


    @Override
    public int find(ByteBuffer key, IndexFunction fn) {
        Block block = root;
        while (true) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(key, IndexFunction.FLOOR);
                if (i == -1) {
                    return -1;
                }
                int blockIdx = block.blockIndex(i);
                block = loadBlock(blockIdx);
            } else { //leaf node
                int i = block.find(key, fn);
                if (i == -1) {
                    return -1;
                }
                return block.logPos(i);
            }
        }
    }

    private long logPos(int idx) {

    }

    private Block loadBlock(int idx) {
        if (!readOnly.get()) {
            throw new RuntimeException("Not read only");
        }
        if (idx < 0 || idx >= numBlocks()) {
            throw new IndexOutOfBoundsException(idx);
        }
        int offset = idx * blockSize;
        ByteBuffer readBuffer = mf.buffer().slice(offset, blockSize);
        return Block.from(rowKey, readBuffer);
    }

    @Override
    public int entries() {
        return root.entries();
    }


    @Override
    public void truncate() {
        mf.truncate(mf.position());
    }

    /**
     * Complete this index and mark it as read only.
     */
    @Override
    public void complete() {
        //flush remaining nodes
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
        }

        mf.flush();
        truncate();
        readOnly.set(true);
        this.root = readRoot();
        nodeBlocks.clear();
    }

    private Block readRoot() {
        return loadBlock(numBlocks() - 1);
    }

    @Override
    public void close() {
        mf.close();
    }

    private long align(long maxEntries, int blockSize) {
        List<Long> levelAllocations = new ArrayList<>();
        int totalBlocks = 0;
        long lastLevelItems = maxEntries;
        int level = 0;
        do {
            lastLevelItems = numberOfNodesPerLevel(lastLevelItems, level, blockSize);
            levelAllocations.add(lastLevelItems);
            level++;
            totalBlocks += lastLevelItems;
        } while (lastLevelItems > 1);

        assert lastLevelItems == 1 : "Root must always have one node";

        long alignedSize = totalBlocks * blockSize;

        System.out.println("Blocks: " + totalBlocks + " alignedSize: " + alignedSize);
        System.out.println(levelAllocations);

        return alignedSize;
    }

    private int numberOfNodesPerLevel(long numEntries, int level, int blockSize) {
        int blockEntrySize = level == 0 ? Block.leafEntrySize(rowKey) : Block.internalEntrySize(rowKey);
        int entriesPerBlock = (blockSize - Block.HEADER) / blockEntrySize;
        int blocks = (int) (numEntries / entriesPerBlock);
        blocks += numEntries % entriesPerBlock > 0 ? 1 : 0;
        return blocks;
    }

    public String name() {
        return mf.name();
    }

    public int numBlocks() {
        return mf.position() / blockSize;
    }

    public int size() {
        return mf.capacity();
    }

    private static class BlockCache {

        private final int blockSize;
        private final Queue<ByteBuffer> buffers = new ArrayDeque<>();

        private BlockCache(int blockSize) {
            this.blockSize = blockSize;
        }

        public void free(ByteBuffer buffer) {

        }
    }

}