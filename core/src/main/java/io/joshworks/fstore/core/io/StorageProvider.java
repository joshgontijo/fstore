package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Objects;

public class StorageProvider {

    private final boolean mmap;
    private final int mmapBufferSize;

    private StorageProvider(boolean mmap, int mmapBufferSize) {
        this.mmap = mmap;
        this.mmapBufferSize = mmapBufferSize;
    }

    public static StorageProvider mmap(int bufferSize) {
        if (bufferSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("MMap buffer size must be at least " + Memory.PAGE_SIZE + ", got " + bufferSize);
        }
        return new StorageProvider(true, bufferSize);
    }

    public static StorageProvider raf() {
        return new StorageProvider(false, -1);
    }

    public Storage create(File file, long size) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file, size);
        return mmap ? new MMapStorage(file, raf, mmapBufferSize) : new RafStorage(file, raf);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file);
        return mmap ? new MMapStorage(file, raf, mmapBufferSize) : new RafStorage(file, raf);
    }


}
