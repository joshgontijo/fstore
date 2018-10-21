package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.Writer;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Stream;

public interface Log<T> extends Writer<T>, Closeable {

    long START = LogHeader.BYTES;
    byte[] EOL = new byte[RecordHeader.MAIN_HEADER]; //eof header, -1 length, 0 crc

    String name();

    Stream<T> stream(Direction direction);

    LogIterator<T> iterator(long position, Direction direction);

    LogIterator<T> iterator(Direction direction);

    long position();

    Marker marker();

    T get(long position);

    PollingSubscriber<T> poller(long position);

    PollingSubscriber<T> poller();

    long fileSize();

    long actualSize();

    Set<TimeoutReader> readers();

    SegmentState rebuildState(long lastKnownPosition);

    void delete();

    void roll(int level);

    void roll(int level, ByteBuffer footer);

    ByteBuffer readFooter();

    boolean readOnly();

    long entries();

    int level();

    long created();

    Type type();

}
