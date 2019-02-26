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

        int tobeRead = dst.remaining();
        int read = 0;
        do
        { //READS ARE NOT GUARANTEED TO SEE CHANGES MADE TO THE UNDERLYING FILE, RE-READ IS NEEDED UNTIL ALL DATA IS AVAILABLE
            read += super.read(readPos, dst);
            readPos += read;
        } while (read > 0 && read < tobeRead && (readPos < diskStorage.writePosition()));
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