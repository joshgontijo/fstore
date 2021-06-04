package io.joshworks.fstore.core.iterators;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    @Override
    void close();
}
