package io.joshworks.es2.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.mmap.MappedFile;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public class BTreeIndexSegment {

    static final int BLOCK_SIZE = Memory.PAGE_SIZE;

    private final MappedFile mf;
    private final Block root;

    public static BTreeIndexSegment open(File file) {
        try {
            if (!file.exists()) {
                throw new RuntimeIOException("File does not exist");
            }
            MappedFile mf = MappedFile.open(file, FileChannel.MapMode.READ_ONLY);
            long fileSize = mf.capacity();
            if (fileSize % BLOCK_SIZE != 0) {
                throw new IllegalStateException("Invalid index file length: " + fileSize);
            }

            return new BTreeIndexSegment(mf);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to initialize index", e);
        }
    }

    private BTreeIndexSegment(MappedFile mf) {
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

}