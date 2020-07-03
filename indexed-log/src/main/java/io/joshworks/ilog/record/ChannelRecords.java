package io.joshworks.ilog.record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChannelRecords extends AbstractChannelRecords {

    private ReadableByteChannel src;

    ChannelRecords(String poolName) {
        super(poolName);
    }

    void init(int bufferSize, StripedBufferPool pool, ReadableByteChannel src) {
        super.init(bufferSize, pool);
        this.src = src;
    }

    @Override
    protected int read(ByteBuffer readBuffer) throws IOException {
        return src.read(readBuffer);
    }

    @Override
    public void close() {
        super.close();
        this.src = null;
    }
}
