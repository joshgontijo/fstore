package io.joshworks.ilog.record;

import java.nio.channels.GatheringByteChannel;
import java.util.function.Consumer;

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
    public long writeTo(GatheringByteChannel channel, Consumer<Record2> onWritten) {
        return 0;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count, Consumer<Record2> onWritten) {
        return 0;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public int size() {
        return 0;
    }
}
