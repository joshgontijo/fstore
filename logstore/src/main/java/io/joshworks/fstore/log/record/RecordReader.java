package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.Storage;

public interface RecordReader {

    BufferRef read(Storage storage, BufferPool bufferPool, long position, long logEnd);

}
