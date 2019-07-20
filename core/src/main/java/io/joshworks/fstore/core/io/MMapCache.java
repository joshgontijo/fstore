package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class MMapCache extends MMapStorage {

    public MMapCache(DiskStorage diskStorage) {
        super(diskStorage);
    }

    @Override
    public int write(ByteBuffer src) {
        ensureCapacity(position(), src.remaining());
        return diskStorage.write(src);
    }

    @Override
    public int write(long position, ByteBuffer src) {
        ensureCapacity(position, src.remaining());
        return diskStorage.write(position, src);
    }

    @Override
    public long write(ByteBuffer[] srcs) {
        long srcTotal = 0;
        for (ByteBuffer src : srcs) {
            srcTotal += src.remaining();
        }
        ensureCapacity(position(), srcTotal);
        return diskStorage.write(srcs);
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {
        long dstRemaining = dst.remaining();
        if (dstRemaining == 0) {
            return 0;
        }
        long srcAvailable = diskStorage.position() - readPos;
        if (srcAvailable <= 0) {
            return EOF;
        }
        return super.read(readPos, dst);
    }

    @Override
    public void position(long position) {
        diskStorage.position(position);
        super.position(position);
    }

    @Override
    public long position() {
        return diskStorage.position();
    }

    @Override
    public void truncate(long newSize) {
        super.truncate(newSize);
        diskStorage.truncate(newSize);
    }
}