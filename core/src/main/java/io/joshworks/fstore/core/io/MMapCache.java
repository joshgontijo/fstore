package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class MMapCache extends MMapStorage {

    public MMapCache(DiskStorage diskStorage) {
        super(diskStorage);
    }

    public MMapCache(DiskStorage diskStorage, int bufferSize) {
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
        } while (read > 0 && read < tobeRead && (readPos < this.position.get()));
        return read;
    }

    @Override
    public void writePosition(long position) {
        try {
            diskStorage.writePosition(position);
            super.writePosition(position);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long writePosition() {
        return diskStorage.writePosition();
    }
}