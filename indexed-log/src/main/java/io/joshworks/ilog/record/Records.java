package io.joshworks.ilog.record;

import java.io.Closeable;
import java.nio.channels.GatheringByteChannel;

public interface Records extends Closeable {

    Records EMPTY = new EmptyRecords();

    Record2 poll();

    Record2 peek();


    boolean hasNext();

    @Override
    void close();

    int size();

    long writeTo(GatheringByteChannel channel);

    long writeTo(GatheringByteChannel channel, int offset, int count);
}
