package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class MMapCache extends MMapStorage {

    MMapCache(DiskStorage diskStorage) {
        super(diskStorage);
    }

    MMapCache(DiskStorage diskStorage, int bufferSize) {
        super(diskStorage, bufferSize);
    }

    @Override
    public int write(ByteBuffer src) {
        return diskStorage.write(src);
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {
        long dstRemaining = dst.remaining();
        if (dstRemaining == 0) {
            return 0;
        }
        long srcAvailable = diskStorage.writePosition() - readPos;
        if(srcAvailable <= 0) {
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
    public void writePosition(long position) {
        diskStorage.writePosition(position);
        super.writePosition(position);
    }

    @Override
    public long writePosition() {
        return diskStorage.writePosition();
    }
}