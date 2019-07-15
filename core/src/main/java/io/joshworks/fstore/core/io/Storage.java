package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Memory;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;

public interface Storage extends Flushable, Closeable {

    int EOF = -1;

    /**
     * RELATIVE WRITE
     * Writes to current position and increment the written bytes to its internal position.
     * File will expand to accommodate the new data
     *
     * @param src The data to be written
     * @return The written bytes
     */
    int write(ByteBuffer src);

    /**
     * ABSOLUTE WRITE
     * Writes to the absolute provide position and does not update the internal position.
     * File will expand to accommodate the new data.
     * Writes with position greater than current internal position will expand file to accommodate new data
     *
     * @param position The position the should be written
     * @param src      The data to be written
     * @return The written bytes
     */
    int write(long position, ByteBuffer src);

    /**
     * ABSOLUTE GATHERING WRITE
     * Writes to current position and increment the written bytes to its internal position.
     * File will expand to accommodate the new data
     *
     * @param src The data to be written
     * @return The written bytes
     */
    long write(ByteBuffer[] src);


    /**
     * ABSOLUTE READ
     * Reads from a given position, if position is greater than the current internal position, then {@link Storage#EOF} is returned.
     * Position must be greater or equals zero.
     * Bytes read will be equals the minimum of data available between source and destination.
     *
     * @param position The position the should be written
     * @param dst      The data destination {@link ByteBuffer} to write the data into
     * @return The read bytes
     */
    int read(long position, ByteBuffer dst);

    /**
     * The current underlying store length
     */
    long length();

    /**
     * Sets the current position for the store
     * File length will remain unchanged until a write is performed.
     *
     * @param position A position equals or greater than zero.
     */
    void position(long position);

    /**
     * The current position of write pointer
     */
    long position();

    /**
     * Delete and release any locks associated with this store
     */
    void delete();

    /**
     * The store name
     */
    String name();

    /**
     * Truncates this channel's file to the given size.
     * If the given size is less than the file's current size then the file is truncated,
     * discarding any bytes beyond the new end of the file.
     * If the given size is greater than or equal to the file's current size then the file is not modified.
     * In either case, if this channel's file position is greater than the given size then it is set to that size.
     */
    void truncate(long newSize);

    default boolean hasAvailableData(long readPos) {
        long writePos = position();
        return readPos < writePos;
    }


    static void ensureNonEmpty(ByteBuffer src) {
        if (src.remaining() == 0) {
            throw new StorageException("Cannot store empty record");
        }
    }

    static Storage createOrOpen(File file, StorageMode mode, long size) {
        return Files.exists(file.toPath()) ? open(file, mode) : create(file, mode, size);
    }

    static Storage create(File file, StorageMode mode, long size) {
        requireNonNull(file, "File must be provided");
        requireNonNull(mode, "StorageMode must be provided");
        if (size <= 0) {
            throw new IllegalArgumentException("Storage size must be greater than zero");
        }
        return getStorage(file, mode, size);
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

    static long align(long length) {
        if (length % Memory.PAGE_SIZE == 0) {
            return length;
        }
        return Memory.PAGE_SIZE * ((length / Memory.PAGE_SIZE) + 1);
    }

}
