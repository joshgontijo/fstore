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
            int maxEntrySize,
            double checksumProb,
            int readPageSize);


}
