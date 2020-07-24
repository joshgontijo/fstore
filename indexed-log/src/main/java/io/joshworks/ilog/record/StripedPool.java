package io.joshworks.ilog.record;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface StripedPool extends Closeable {

    ByteBuffer allocate(int size);

    void free(ByteBuffer buffer);

    @Override
    void close();
}
