package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;

public interface Reader {

    <T> RecordEntry<T> read(Storage storage, long position, Serializer<T> serializer);

}
