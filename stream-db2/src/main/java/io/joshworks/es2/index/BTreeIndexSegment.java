package io.joshworks.es2.index;

import io.joshworks.fstore.core.io.mmap.MappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;


public class BTreeIndexSegment {

    private static final Logger log = LoggerFactory.getLogger(BTreeIndexSegment.class);

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


    public void append(long stream, int version, long logPos) {
        assert !isFull() : "Index is full";

        Block node = getOrAllocate(0);
        if (!node.add(stream, version, logPos)) {
            writeNode(node);
            node.add(stream, version, logPos);
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
            nodeBlocks.add(level, new Block(blockSize, level));
        }
        return nodeBlocks.get(level);
    }

    //only works with equals
    public List<IndexEntry> findBatch(long stream, int version, int maxItems) {
        List<IndexEntry> entries = new ArrayList<>();
        Block block = root;
        while (entries.size() < maxItems) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(stream, version, IndexFunction.FLOOR);
                if (i == -1) {
                    return entries;
                }
                int blockIdx = block.blockIndex(i);
                block = loadBlock(blockIdx);
            } else { //leaf node
                int i = block.find(stream, version, IndexFunction.EQUALS);
                if (i == -1) {
                    return entries;
                }
                long foundStream = block.stream(i);
                version = block.version(i);
                long logPos = block.logPos(i);

                entries.add(new IndexEntry(foundStream, version, logPos));

                for (int j = i + 1; j < block.entries() && entries.size() < maxItems; j++) {
                    long nextStream = block.stream(j);
                    int nextVersion = block.version(j);
                    if (nextStream != stream || nextVersion != version + 1) {
                        break;
                    }
                    entries.add(block.toIndexEntry(j));
                    version++;
                }
            }
        }
        return entries;
    }


    public IndexEntry find(long stream, int version, IndexFunction fn) {
        Block block = root;
        while (true) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(stream, version, IndexFunction.FLOOR);
                if (i == -1) {
                    return null;
                }
//                long stream = block.stream(i);
//                int version = block.version(i);
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


    public boolean isFull() {
        return mf.position() >= mf.capacity();
    }


    public int entries() {
        return root.entries();
    }


    public void delete() {
        mf.delete();
//        filter.delete();
    }


    public File file() {
        return mf.file();
    }


    public void truncate() {
        mf.truncate(mf.position());
    }

    /**
     * Complete this index and mark it as read only.
     */

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

        log.info("Blocks: {}, alignedSize: {}, level blocks: {}", totalBlocks, alignedSize, levelAllocations);
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