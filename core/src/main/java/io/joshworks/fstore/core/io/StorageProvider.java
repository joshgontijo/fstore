package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.StatsStorage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;

public class StorageProvider {

    private final StorageMode mode;

    private StorageProvider(StorageMode mode) {
        this.mode = mode;
    }

    public static StorageProvider of(StorageMode mode) {
        return new StorageProvider(mode);
    }

    public Storage create(File file, long size) {
        requireNonNull(file, "File must be provided");
        long alignedSize = align(size);
        RandomAccessFile raf = IOUtils.randomAccessFile(file, alignedSize);
        Storage storage = getStorage(file, raf);
        return new StatsStorage(storage);
    }

    public Storage open(File file) {
        requireNonNull(file, "File must be provided");
        if (!Files.exists(file.toPath())) {
            throw new IllegalStateException("File " + file.getName() + " doesn't exist");
        }
        if (file.length() <= 0) {
            throw new IllegalStateException("File " + file.getName() + " has length equals to zero");
        }
        RandomAccessFile raf = IOUtils.randomAccessFile(file);
        Storage storage = getStorage(file, raf);
        storage.position(file.length());
        return new StatsStorage(storage);
    }

    private Storage getStorage(File file, RandomAccessFile raf) {
        switch (mode) {
            case MMAP:
                return new MMapStorage(file, raf);
            case RAF:
                return new RafStorage(file, raf);
            case RAF_CACHED:
                return new MMapCache(file, raf);
            default:
                throw new IllegalArgumentException("Invalid storage mode: " + mode);
        }
    }


    private static long align(long fileSize) {
        if (fileSize % Memory.PAGE_SIZE == 0) {
            return fileSize;
        }
        return Memory.PAGE_SIZE * ((fileSize / Memory.PAGE_SIZE) + 1);
    }


}
