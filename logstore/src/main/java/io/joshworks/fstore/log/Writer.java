package io.joshworks.fstore.log;

public interface Writer<T> {

    long append(T data);

    void flush();

}
