package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.log.Direction;

public interface DataStream {

    DataReader reader(Direction direction, BufferPool bufferPool);

}
