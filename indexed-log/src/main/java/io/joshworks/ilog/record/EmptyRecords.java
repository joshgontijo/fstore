package io.joshworks.ilog.record;

import io.joshworks.ilog.IndexedSegment;

import java.nio.channels.GatheringByteChannel;

class EmptyRecords implements Records {

    @Override
    public Record2 poll() {
        return null;
    }

    @Override
    public Record2 peek() {
        return null;
    }

    @Override
    public long writeTo(IndexedSegment segment) {
        return 0;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        return 0;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        return 0;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {

    }
}
