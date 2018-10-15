package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.IDataStream;

public interface SegmentFactory<T, L extends Log<T>> {

    L createOrOpen(Storage storage, Serializer<T> serializer, IDataStream reader, String magic, Type type);


}
