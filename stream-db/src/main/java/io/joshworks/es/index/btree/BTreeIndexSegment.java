package io.joshworks.es.index.btree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.index.IndexSegment;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class BTreeIndexSegment implements IndexSegment {

    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final int blockSize;

    //read path
    private Block root;

    //write path
    private final List<Block> nodeBlocks = new ArrayList<>();

    public BTreeIndexSegment(File file, long numEntries, int blockSize) {
        this.blockSize = blockSize;
        try {
            boolean newFile = file.createNewFile();
            if (newFile) {
                long alignedSize = align(numEntries, blockSize);
                this.mf = MappedFile.create(file, alignedSize);
            } else { //existing file
                this.mf = MappedFile.open(file);
                long fileSize = mf.capacity();
                if (fileSize % blockSize != 0) {
                    throw new IllegalStateException("Invalid index file length: " + fileSize);
                }
                readOnly.set(true);
                this.root = readRoot();
            }

        } catch (IOException ioex) {
            throw new RuntimeException("Failed to create index", ioex);
        }
    }

    @Override
    public void append(long stream, int version, int size, long logPos) {
        assert !isFull() : "Index is full";

        Block node = getOrAllocate(0);
        if (!node.add(stream, version, size, logPos)) {
            writeNode(node);
            node.add(stream, version, size, logPos);
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
        int idx = node.writeTo(mf);
    }


    private Block getOrAllocate(int level) {
        if (level >= nodeBlocks.size()) {
            nodeBlocks.add(level, new Block(blockSize, level));
        }
        return nodeBlocks.get(level);
    }

    @Override
    public IndexEntry find(IndexKey key, IndexFunction fn) {
        ByteBuffer kb = Buffers.allocate(IndexKey.BYTES, false);
        kb.putLong(key.stream()).putInt(key.version()).flip();

        Block block = root;
        while (true) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(key, IndexFunction.FLOOR);
                if (i == -1) {
                    return null;
                }
//                long stream = block.stream(i);
//                int version = block.version(i);
                int blockIdx = block.blockIndex(i);
                block = loadBlock(blockIdx);
            } else { //leaf node
                int i = block.find(key, fn);
                if (i == -1) {
                    return null;
                }
                long stream = block.stream(i);
                int version = block.version(i);
                int size = block.recordSize(i);
                long logPos = block.logPos(i);

                return new IndexEntry(stream, version, size, logPos);
            }
        }
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
        return Block.from(readBuffer);
    }

    @Override
    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }

    @Override
    public int entries() {
        return root.entries();
    }

    @Override
    public void delete() {
        mf.delete();
//        filter.delete();
    }

    @Override
    public File file() {
        return mf.file();
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

        assert mf.position() == mf.capacity() : "Block count was not correct";

        mf.flush();
        truncate();
        readOnly.set(true);
        this.root = readRoot();
        nodeBlocks.clear();
    }

    private List<Block> sortedNodes() {
        return nodeBlocks
                .stream()
                .sorted(Comparator.comparingInt(Block::level))
                .collect(Collectors.toList());
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
        int blockEntrySize = level == 0 ? Block.LEAF_ENTRY_BYTES : Block.INTERNAL_ENTRY_BYTES;
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

}