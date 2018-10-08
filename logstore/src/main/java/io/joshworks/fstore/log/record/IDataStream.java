package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;

public interface IDataStream {

    long write(Storage storage, BufferPool bufferPool, ByteBuffer data);

    BufferRef read(Storage storage, BufferPool bufferPool, Direction direction, long position);

    BufferRef bulkRead(Storage storage, BufferPool bufferPool, Direction direction, long position);

}
