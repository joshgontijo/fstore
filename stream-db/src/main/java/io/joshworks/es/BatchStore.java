package io.joshworks.es;

import io.joshworks.es.async.WriteEvent;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BatchStore implements IEventStore{

    private final IEventStore delegate;

    private final Map<Long, Integer> versionCache = new ConcurrentHashMap<>();

    public BatchStore(IEventStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public int version(long stream) {
        return 0;
    }

    @Override
    public void linkTo(long srcStream, int srcVersion, long dstStream, int expectedVersion) {

    }

    @Override
    public void append(WriteEvent event) {

    }

    @Override
    public int get(long stream, int version, ByteBuffer dst) {
        return 0;
    }
}
