package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.Writer;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.Closeable;

public interface Log<T> extends Writer<T>, Closeable {

    long START = LogHeader.BYTES;
    byte[] EOL = new byte[RecordHeader.MAIN_HEADER]; //eof header, -1 length, 0 crc

    String name();

    SegmentIterator<T> iterator(long position, Direction direction);

    SegmentIterator<T> iterator(Direction direction);

    long position();

    T get(long position);

    long fileSize();

    SegmentState rebuildState(long lastKnownPosition);

    void delete();

    void roll(int level);

    boolean readOnly();

    long entries();

    int level();

    long created();

    Type type();

}
