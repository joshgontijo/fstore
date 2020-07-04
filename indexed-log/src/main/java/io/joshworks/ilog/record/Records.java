package io.joshworks.ilog.record;

import io.joshworks.ilog.IndexedSegment;

import java.io.Closeable;
import java.nio.channels.GatheringByteChannel;

public interface Records extends Closeable {

    Records EMPTY = new EmptyRecords();

    Record2 poll();

    Record2 peek();

    long writeTo(IndexedSegment segment);

    long writeTo(GatheringByteChannel channel);

    long writeTo(GatheringByteChannel channel, int count);

    boolean hasNext();

    @Override
    void close();
}
