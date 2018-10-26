package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.StatsStorage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Objects;

public class StorageProvider {

    private final Mode mode;

    private StorageProvider(Mode mode) {
        this.mode = mode;
    }

    public static StorageProvider of(Mode mode) {
        return new StorageProvider(mode);
    }

    public Storage create(File file, long size) {
        if (Mode.OFF_HEAP.equals(mode)) {
            return new OffHeapStorage(size);
        }
        Objects.requireNonNull(file, "File must be provided");
        long alignedSize = align(size);
        Storage storage = createStorage(file, alignedSize);
        return new StatsStorage(storage);
    }

    public Storage open(File file) {
        if (!Files.exists(file.toPath())) {
            throw new IllegalStateException("File " + file.getName() + " doesn't exist");
        }
        if (file.length() <= 0) {
            throw new IllegalStateException("File " + file.getName() + " has length equals to zero");
        }
        Objects.requireNonNull(file, "File must be provided");
        Storage storage = openStorage(file);
        return new StatsStorage(storage);
    }

    private Storage createStorage(File file, long size) {
        RandomAccessFile raf;
        switch (mode) {
            case MMAP:
                raf = IOUtils.randomAccessFile(file, size);
                return new MMapStorage(file, raf);
            case RAF:
                raf = IOUtils.randomAccessFile(file, size);
                return new RafStorage(file, raf);
            case OFF_HEAP:
                return new OffHeapStorage(size);
            case RAF_CACHED:
                raf = IOUtils.randomAccessFile(file, size);
                return new MMapCache(file, raf);
        }
        throw new IllegalArgumentException("No valid storage mode " + mode);
    }

    private Storage openStorage(File file) {
        RandomAccessFile raf;
        switch (mode) {
            case MMAP:
                raf = IOUtils.randomAccessFile(file);
                return new MMapStorage(file, raf);
            case RAF:
                raf = IOUtils.randomAccessFile(file);
                return new RafStorage(file, raf);
            case OFF_HEAP:
                return new OffHeapStorage(file.length());
            case RAF_CACHED:
                raf = IOUtils.randomAccessFile(file);
                return new MMapCache(file, raf);
        }
        throw new IllegalArgumentException("No valid storage mode " + mode);
    }

    private static long align(long fileSize) {
        if (fileSize % Memory.PAGE_SIZE == 0) {
            return fileSize;
        }
        return Memory.PAGE_SIZE * ((fileSize / Memory.PAGE_SIZE) + 1);
    }


}
