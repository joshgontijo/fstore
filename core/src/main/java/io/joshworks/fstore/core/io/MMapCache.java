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

    //READS ARE NOT GUARANTEED TO SEE CHANGES MADE TO THE UNDERLYING FILE, RE-READ IS NEEDED UNTIL ALL DATA IS AVAILABLE
    @Override
    public int read(long readPos, ByteBuffer dst) {
        if (this.position.get() <= readPos) { // no data available
            return 0;
        }
        int tobeRead = dst.remaining();
        int position = dst.position();
        int read;
        do {
            //TODO getting stuck, SEEMS data is not all being writen.
            //only reading 80 bytes on 'store_must_support_concurrent_reads_and_writes'
            dst.position(position);
            read = super.read(readPos, dst);
        } while (read < tobeRead);
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