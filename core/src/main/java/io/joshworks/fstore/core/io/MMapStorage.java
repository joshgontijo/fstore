package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.MappedByteBuffers;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MMapStorage extends MemStorage {

    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    protected final DiskStorage diskStorage;

    MMapStorage(DiskStorage diskStorage) {
        super(diskStorage.name(), diskStorage.length(), (from, size) -> map(diskStorage, from, size));
        this.diskStorage = diskStorage;
    }

    MMapStorage(DiskStorage diskStorage, int bufferSize) {
        super(diskStorage.name(), diskStorage.length(), bufferSize, (from, size) -> map(diskStorage, from, size));
        this.diskStorage = diskStorage;
    }

    protected static ByteBuffer map(DiskStorage diskStorage, long from, int size) {
        try {
            return diskStorage.channel.map(FileChannel.MapMode.READ_WRITE, from, size);
        } catch (Exception e) {
            throw new StorageException("Failed to map buffer from: " + from + ", size: " + size, e);
        }
    }

    @Override
    protected void destroy(ByteBuffer buffer) {
        MappedByteBuffers.unmap((MappedByteBuffer) buffer);
    }

    @Override
    public void flush() {
        long pos = this.position.get();
        int idx = bufferIdx(pos);
        if (idx >= numBuffers()) {
            return;
        }
        MappedByteBuffer buffer = (MappedByteBuffer) getBuffer(pos);
        if (buffer != null && !isWindows) {
            //caused by https://bugs.openjdk.java.net/browse/JDK-6539707
            buffer.force();
        }
    }

    @Override
    public void close() {
        super.close();
        diskStorage.close();
    }

    @Override
    public void writePosition(long position) {
        super.writePosition(position);
        diskStorage.writePosition(position);
    }

    @Override
    public void delete() {
        super.delete();
        diskStorage.delete();
    }

}