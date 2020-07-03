package io.joshworks.ilog.record;

import java.io.Closeable;
import java.nio.channels.GatheringByteChannel;

public interface Records extends Closeable {
    Record2 poll();

    Record2 peek();

    long writeTo(GatheringByteChannel channel);

    long writeTo(GatheringByteChannel channel, int count);

    String poolName();

    boolean hasNext();
}
