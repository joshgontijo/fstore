package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;

public interface DataStream {

    int write(Storage storage, BufferPool bufferPool, ByteBuffer data);

    DataReader reader(Storage storage, BufferPool bufferPool, Direction direction);

}
