package io.joshworks.es;

import io.joshworks.es.async.WriteEvent;
import io.joshworks.es.index.IndexKey;

import java.nio.ByteBuffer;

public interface IEventStore {

    int version(long stream);

    void linkTo(String srcStream, int srcVersion, String dstStream, int expectedVersion);

    void append(WriteEvent event);

    int get(IndexKey key, ByteBuffer dst);
}
