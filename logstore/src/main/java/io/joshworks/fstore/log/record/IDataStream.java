package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;

public interface IDataStream {

    long write(Storage storage, ByteBuffer data);

    <T> long write(Storage storage, T data, Serializer<T> serializer);

    BufferRef read(Storage storage, Direction direction, long position);

    BufferRef bulkRead(Storage storage, Direction direction, long position);

}
