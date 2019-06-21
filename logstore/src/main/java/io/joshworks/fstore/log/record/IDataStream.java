package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;
import java.util.List;

public interface IDataStream {

    long write(Storage storage, ByteBuffer data);

    <T> RecordEntry<T> read(Storage storage, Direction direction, long position, Serializer<T> serializer);

    <T> List<RecordEntry<T>> bulkRead(Storage storage, Direction direction, long position, Serializer<T> serializer);

}
