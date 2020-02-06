package io.joshworks.fstore.core.util;

public interface Pool<T> {

    T allocate();

    void free(T element);

}
