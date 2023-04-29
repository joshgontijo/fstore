package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.io.File;

public interface SegmentFactory<T> {

    Log<T> createOrOpen(
            File file,
            StorageMode storageMode,
            long dataLength,
            Serializer<T> serializer,
            BufferPool bufferPool,
            WriteMode writeMode,
            double checksumProb);


    //used specifically for merging adjacent segments, includes addition information about entries
    default Log<T> mergeOut(File file,
                            StorageMode storageMode,
                            long dataLength,
                            long entries,
                            Serializer<T> serializer,
                            BufferPool bufferPool,
                            WriteMode writeMode,
                            double checksumProb) {

        return createOrOpen(file, storageMode, dataLength, serializer, bufferPool, writeMode, checksumProb);
    }

}
