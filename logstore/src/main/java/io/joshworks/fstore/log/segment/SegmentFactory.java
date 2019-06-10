package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.header.Type;

public interface SegmentFactory<T> {

    Log<T> createOrOpen(Storage storage, Serializer<T> serializer, IDataStream reader, String magic, WriteMode mode);


}
