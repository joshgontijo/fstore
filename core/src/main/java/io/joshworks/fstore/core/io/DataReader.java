package io.joshworks.fstore.core.io;

public interface DataReader<T> {

    long position();

    DataReader<T> position(long position);

    T readForward();

    T readBackward();

    boolean head();


}
