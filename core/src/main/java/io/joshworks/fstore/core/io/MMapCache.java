package io.joshworks.fstore.core.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class MMapCache extends MMapStorage {

    public MMapCache(File file, RandomAccessFile raf) {
        super(file, raf);
    }

    @Override
    public int write(ByteBuffer src) {
        return super.writeDirect(src);
    }

    @Override
    public void position(long position) {
        try {
            channel.position(position);
            super.position(position);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
}