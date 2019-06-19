package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class MMapCache extends MMapStorage {

    public MMapCache(DiskStorage diskStorage) {
        super(diskStorage);
    }

    MMapCache(DiskStorage diskStorage, int bufferSize) {
        super(diskStorage, bufferSize);
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
        long toBeRead = Math.min(dstRemaining, srcAvailable);
        int read = 0;
        int lastRead;
        //READS ARE NOT GUARANTEED TO SEE CHANGES MADE TO THE UNDERLYING FILE, RE-READ IS NEEDED UNTIL ALL DATA IS AVAILABLE
        do {
            lastRead = super.read(readPos, dst);
            if (lastRead != EOF) {
                readPos += lastRead;
                read += lastRead;
            }
        } while (read < toBeRead);
        return read;
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