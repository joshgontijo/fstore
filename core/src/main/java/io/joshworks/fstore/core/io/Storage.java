package io.joshworks.fstore.core.io;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;

public interface Storage extends Flushable, Closeable {

    int EOF = -1;

    int write(ByteBuffer data);

    int write(long position, ByteBuffer data);

    long write(ByteBuffer[] src);

    int read(long position, ByteBuffer data);

    long size();

    void position(long position);

    long position();

    void delete();

    String name();

    void truncate(long newSize);

//    //position can be set to fileLength
//    //but if a write is performed, then it should return EOF
//    default void validateWriteAddress(long position) {
//        if (position < 0 || position > length()) {
//            long size = length();
//            throw new StorageException("Invalid position: " + position + ", valid range: 0 to " + size);
//        }
//    }

    default boolean hasAvailableData(long readPos) {
        long writePos = position();
        return readPos < writePos;
    }


    static void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new StorageException("Cannot store empty record");
        }
    }

    static Storage createOrOpen(File file, StorageMode mode, long size) {
        return Files.exists(file.toPath()) ? open(file, mode) : create(file, mode, size);
    }

    static Storage create(File file, StorageMode mode, long size) {
        requireNonNull(file, "File must be provided");
        if (size <= 0) {
            throw new IllegalArgumentException("Storage size must be greater than zero");
        }
        Storage storage = getStorage(file, mode, size);
        return storage;
    }

    static Storage open(File file, StorageMode mode) {
        requireNonNull(file, "File must be provided");
        if (!Files.exists(file.toPath())) {
            throw new IllegalStateException("File " + file.getName() + " doesn't exist");
        }
        if (file.length() <= 0) {
            throw new IllegalStateException("File " + file.getName() + " has length equals to zero");
        }
        Storage storage = getStorage(file, mode, file.length());
        storage.position(file.length());
        return storage;
    }

    private static Storage getStorage(File file, StorageMode mode, long alignedSize) {
        switch (mode) {
            case MMAP:
                DiskStorage diskStorage1 = new DiskStorage(file, alignedSize, IOUtils.randomAccessFile(file, alignedSize));
                return new MMapStorage(diskStorage1);
            case RAF:
                return new DiskStorage(file, alignedSize, IOUtils.randomAccessFile(file, alignedSize));
            case RAF_CACHED:
                DiskStorage diskStorage2 = new DiskStorage(file, alignedSize, IOUtils.randomAccessFile(file, alignedSize));
                return new MMapCache(diskStorage2);
            case OFF_HEAP:
                return new OffHeapStorage(file.getAbsolutePath(), alignedSize);
            case HEAP:
                return new HeapStorage(file.getName(), alignedSize);
            default:
                throw new IllegalArgumentException("Invalid storage mode: " + mode);
        }
    }

}
