package io.joshworks.fstore.core.io;

public interface RecordReader<T> {

    long position();

    RecordReader<T> position(long position);

    T readNext();
}
