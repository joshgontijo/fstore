package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.segment.Log;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public interface Appender<T, L extends Log<T>> extends Closeable {
    void roll();

    void compact();

    long append(T data);

    String name();

    //TODO implement reader pool, instead using a new instance of reader, provide a pool of reader to better performance
    LogIterator<T> iterator(Direction direction);

    Stream<T> stream(Direction direction);

    LogIterator<T> iterator(long position, Direction direction);

    PollingSubscriber<T> poller();

    PollingSubscriber<T> poller(long position);

    long position();

    T get(long position);

    long size();

    LogIterator<L> segments(Direction direction);

    Stream<L> streamSegments(Direction direction);


    long size(int level);

    void close();

    void flush();

    List<String> segmentsNames();

    long entries();

    String currentSegment();

    int depth();

    Path directory();
}
