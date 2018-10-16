package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.DiskStorage;
import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class StorageProvider {

    private final boolean mmap;
    private final int mmapBufferSize;
    private final long maxCacheSize;

    private final AtomicLong cacheAvailable = new AtomicLong();

    private StorageProvider(boolean mmap, int mmapBufferSize, long maxCacheSize) {
        this.mmap = mmap;
        this.mmapBufferSize = mmapBufferSize;
        this.maxCacheSize = maxCacheSize;
    }

    static StorageProvider mmap(int bufferSize) {
        if(bufferSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("MMap buffer size must be at least " + Memory.PAGE_SIZE + ", got " + bufferSize);
        }
        return new StorageProvider(true, bufferSize, Config.NO_CACHING);
    }

    static int mmapBufferSize(int bufferSize, long segmentSize) {
        if(bufferSize < 0) { // not provided
            return segmentSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) segmentSize;
        }
        return bufferSize;
    }

    static StorageProvider raf(long maxCacheSize) {
        return new StorageProvider(false, -1, maxCacheSize);
    }

    public Storage create(File file, long length) {
        Objects.requireNonNull(file, "File must be provided");
        return mmap ? new MMapStorage(file, length, Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, length, Mode.READ_WRITE);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        Storage storage = mmap ? new MMapStorage(file, file.length(), Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, file.length(), Mode.READ_WRITE);
        return maxCacheSize == Config.NO_CACHING ? storage : new CachingStorage(storage, this);
    }

    public boolean allocateCache(long size) {
        cacheAvailable.
    }

}
