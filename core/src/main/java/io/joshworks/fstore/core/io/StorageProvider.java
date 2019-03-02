package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.StatsStorage;

import java.io.File;
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
        if (size <= 0) {
            throw new IllegalArgumentException("Storage size must be greater than zero");
        }
        Storage storage = getStorage(file, size);
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
        Storage storage = getStorage(file, file.length());
        storage.writePosition(file.length());
        return new StatsStorage(storage);
    }

    private Storage getStorage(File file, long alignedSize) {
        switch (mode) {
            case MMAP:
                RafStorage diskStorage1 = new RafStorage(file, IOUtils.randomAccessFile(file, alignedSize));
                return new MMapStorage(diskStorage1);
            case RAF:
                return new RafStorage(file, IOUtils.randomAccessFile(file, alignedSize));
            case RAF_CACHED:
                RafStorage diskStorage2 = new RafStorage(file, IOUtils.randomAccessFile(file, alignedSize));
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
