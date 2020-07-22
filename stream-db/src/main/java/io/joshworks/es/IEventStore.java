package io.joshworks.es;

import io.joshworks.es.async.WriteEvent;

import java.nio.ByteBuffer;

public interface IEventStore {

    int version(long stream);

    void linkTo(long srcStream, int srcVersion, long dstStream, int expectedVersion);

    void append(WriteEvent event);

    int get(long stream, int version, ByteBuffer dst);
}
