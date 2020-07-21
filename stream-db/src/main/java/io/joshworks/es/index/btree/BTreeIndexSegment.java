package io.joshworks.es.index.btree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.index.IndexSegment;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


public class BTreeIndexSegment implements IndexSegment {

    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final int blockSize;

    //read path
    private Block root;

    //write path
    private final Map<Integer, Block> nodeBlocks = new HashMap<>();

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
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }

        Block node = getOrAllocate(0);
        if (!node.add(stream, version, size, logPos)) {
            writeNode(node);
            node.add(stream, version, size, logPos);
        }
    }

    public void writeNode(Block node) {
        int idx = node.writeTo(mf);
        addNodeLink(node, idx);
    }

    private void addNodeLink(Block node, int blockIdx) {
        Block parent = getOrAllocate(node.level() + 1);
        if (!parent.addLink(node, blockIdx)) {
            writeNode(parent);
            parent.addLink(node, blockIdx);
        }
    }

    private Block getOrAllocate(int level) {
        return nodeBlocks.compute(level, (k, v) -> v == null ? new Block(blockSize, k) : v);
    }

    @Override
    public IndexEntry find(IndexKey key, IndexFunction fn) {
        Block block = root;
        while (true) {
            if (block.level() > 0) { //internal node use floor
                int i = block.find(key, IndexFunction.FLOOR);
                if (i == -1) {
                    return null;
                }
                long stream = block.stream(i);
                int version = block.version(i);
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
        try {
            mf.delete();
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to delete index");
        }
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
        //higher levels will always be at the end
        //root will always be last
        nodeBlocks.values()
                .stream()
                .filter(Block::hasData)
                .sorted(Comparator.comparingInt(Block::level))
                .forEach(this::writeNode);

        mf.flush();
        truncate();
        readOnly.set(true);
        this.root = readRoot();

    }

    private Block readRoot() {
        return loadBlock(numBlocks() - 1);
    }

    @Override
    public void close() {
        mf.close();
    }

    private long align(long maxEntries, int blockSize) {
        int leafBlocks = numberOfBlocks(maxEntries, blockSize, Block.LEAF_ENTRY_BYTES);
        int internalBlocks = numberOfBlocks(leafBlocks, blockSize, Block.INTERNAL_ENTRY_BYTES);

        int totalNumberOfBlocks = leafBlocks + internalBlocks + 1; // +1 is for the root
        long alignedSize = totalNumberOfBlocks * blockSize;
        System.out.println("Blocks: " + totalNumberOfBlocks + " alignedSize: " + alignedSize);
        return alignedSize;
    }

    private int numberOfBlocks(long numEntries, int blockSize, int blockEntrySize) {
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