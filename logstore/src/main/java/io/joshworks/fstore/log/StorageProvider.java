package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.MMapStorage;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.util.Objects;

public class StorageProvider {

    private final long segmentSize;
    private final boolean mmap;
    private final int mmapBufferSize;

    private StorageProvider(long segmentSize, boolean mmap, int mmapBufferSize) {
        this.segmentSize = segmentSize;
        this.mmap = mmap;
        this.mmapBufferSize = mmapBufferSize;
    }

    public static StorageProvider mmap(long segmentSize, int bufferSize) {
        if (bufferSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("MMap buffer size must be at least " + Memory.PAGE_SIZE + ", got " + bufferSize);
        }
        return new StorageProvider(segmentSize, true, bufferSize);
    }

    public static StorageProvider raf(long segmentSize) {
        return new StorageProvider(segmentSize, false, -1);
    }

    public Storage createOrOpen(File file) { //file length must be provided
        try (Storage storage = new RafStorage(file) {

        }
        Objects.requireNonNull(file,"File must be provided") ;
        return mmap ? new MMapStorage(file, length, Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, length, Mode.READ_WRITE);
    }

    public Storage create(File file) {
        Objects.requireNonNull(file, "File must be provided");
        return mmap ? new MMapStorage(file, length, Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, length, Mode.READ_WRITE);
    }

    public Storage open(File file) {
        Objects.requireNonNull(file, "File must be provided");
        return mmap ? new MMapStorage(file, file.length(), Mode.READ_WRITE, mmapBufferSize) : new RafStorage(file, file.length(), Mode.READ_WRITE);
    }

}
