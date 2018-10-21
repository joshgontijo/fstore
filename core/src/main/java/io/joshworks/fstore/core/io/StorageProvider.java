package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.StatsStorage;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Objects;

public class StorageProvider {

    private final boolean mmap;
    private final boolean rafCached;

    private StorageProvider(boolean mmap, boolean rafCached) {
        this.mmap = mmap;
        this.rafCached = rafCached;
    }

    public static StorageProvider mmap() {
        return new StorageProvider(true, false);
    }

    public static StorageProvider raf() {
        return raf(false);
    }

    public static StorageProvider raf(boolean cached) {
        return new StorageProvider(false, cached);
    }

    public Storage create(File file, long size) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file, size);
        Storage storage = mmap ? new MMapStorage(file, raf) : getRafStorage(file, raf);
        return new StatsStorage(storage);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        RandomAccessFile raf = IOUtils.randomAccessFile(file);
        Storage storage = mmap ? new MMapStorage(file, raf) : getRafStorage(file, raf);
        return new StatsStorage(storage);
    }

    private Storage getRafStorage(File file, RandomAccessFile raf) {
        return rafCached ? new MMapCache(file, raf) : new RafStorage(file, raf);
    }


}
