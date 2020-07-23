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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class BTreeIndexSegment implements IndexSegment {

    private final MappedFile mf;
    private final AtomicBoolean readOnly = new AtomicBoolean();
    private final int blockSize;

    //read path
    private Block root;

    //write path
    private final Map<Integer, Block> nodeBlocks = new HashMap<>();
    private int blocks;
    private int depth;

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
        blocks++;
    }

    //write not without linking node to the parent, used only for root
    public void writeNodeFinal(Block node) {
        int idx = node.writeTo(mf);
        blocks++;
    }


    private Block getOrAllocate(int level) {
        return nodeBlocks.compute(level, (k, v) -> v == null ? new Block(blockSize, k) : v);
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
        //higher levels will always be at the end
        //root will always be last
        nodeBlocks.values()
                .stream()
                .filter(Block::hasData)
                .sorted(Comparator.comparingInt(Block::level))
                .forEach(this::writeNode);


        //FIXME logic is wrong, root can still create another root without needing it
        //FIXME after writing all nodes, a new root can still be created
        //FIXME total segment size is still broken
        List<Block> nodes;
        do {
            nodes = getRemainingNodes();
            if (nodes.size() == 1) { //root node, write without creating a parent
                Block root = nodes.get(0);
                writeNodeFinal(root);
                break; //done
            }
            for (Block node : nodes) {
                writeNode(node);
            }
        } while (!nodes.isEmpty());

        assert getRemainingNodes().isEmpty() : "Not all nodes were flushed";

        mf.flush();
//        filter.flush();
        truncate();
        readOnly.set(true);
        this.root = readRoot();
        nodeBlocks.clear();
    }

    private List<Block> getRemainingNodes() {
        return nodeBlocks.values()
                .stream()
                .filter(Block::hasData)
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
        int leafBlocks = numberOfBlocks(maxEntries, blockSize, Block.LEAF_ENTRY_BYTES);

        int internalBlocks = numberOfBlocks(leafBlocks, blockSize, Block.INTERNAL_ENTRY_BYTES);

        int totalNumberOfBlocks = leafBlocks + internalBlocks;
        long alignedSize = totalNumberOfBlocks * blockSize;
        System.out.println("Blocks: " + totalNumberOfBlocks + " alignedSize: " + alignedSize);
        return alignedSize;
    }

    private int numberOfInternalBlocks(long leafBlocks, int blockSize, int blockEntrySize) {
        int entriesPerBlock = (blockSize - Block.HEADER) / blockEntrySize;
        int blocks = (int) (numEntries / entriesPerBlock);
        blocks += numEntries % entriesPerBlock > 0 ? 1 : 0;
        return blocks;
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