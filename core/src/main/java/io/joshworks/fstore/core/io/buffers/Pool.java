package io.joshworks.fstore.core.io.buffers;

import java.io.Closeable;

public interface Pool<T> extends Closeable {

    T allocate();

}
