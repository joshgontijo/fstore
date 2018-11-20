package io.joshworks.fstore.core.io;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MMapCache extends MMapStorage {

    public MMapCache(File file, RandomAccessFile raf) {
        super(file, raf);
    }

    @Override
    public int write(ByteBuffer src) {
        return super.writeDirect(src);
    }

    @Override
    public int read(long readPos, ByteBuffer dst) {
        if (this.position.get() <= readPos) { // no data available
            return 0;
        }

        int tobeRead = dst.remaining();
        int read = 0;
        do { //READS ARE NOT GUARANTEED TO SEE CHANGES MADE TO THE UNDERLYING FILE, RE-READ IS NEEDED UNTIL ALL DATA IS AVAILABLE
            read += super.read(readPos, dst);
            readPos += read;
        } while (read > 0 && read < tobeRead);
        return read;
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