package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;

public interface IDataStream {

    long write(Storage storage, ByteBuffer data);

    BufferRef read(Storage storage, Direction direction, long position);

    BufferRef bulkRead(Storage storage, Direction direction, long position);

}
