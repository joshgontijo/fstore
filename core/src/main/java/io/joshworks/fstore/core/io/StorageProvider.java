package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Objects;

public class StorageProvider {

    private final boolean mmap;
    private final boolean rafCached;
    private final int mmapBufferSize;

    private StorageProvider(boolean mmap, int mmapBufferSize, boolean rafCached) {
        this.mmap = mmap;
        this.mmapBufferSize = mmapBufferSize;
        this.rafCached = rafCached;
    }

    public static StorageProvider mmap(int bufferSize) {
        if (bufferSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("MMap buffer size must be at least " + Memory.PAGE_SIZE + ", got " + bufferSize);
        }
        return new StorageProvider(true, bufferSize, false);
    }

    public static StorageProvider raf() {
        return raf(false);
    }

    public static StorageProvider raf(boolean cached) {
        return new StorageProvider(false, -1, cached);
    }

    public Storage create(File file, long size) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file, size);
        return mmap ? new MMapStorage(file, raf, mmapBufferSize) : getRafStorage(file, raf);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file);
        return mmap ? new MMapStorage(file, raf, mmapBufferSize) : getRafStorage(file, raf);
    }

    private Storage getRafStorage(File file, RandomAccessFile raf) {
        return rafCached ? new MMapCache(file, raf) : new RafStorage(file, raf);
    }


}
