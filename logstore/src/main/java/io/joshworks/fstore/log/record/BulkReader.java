package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.util.List;

public interface BulkReader {

    <T> List<RecordEntry<T>> read(Storage storage, BufferPool bufferPool, long position, Serializer<T> serializer);

}
