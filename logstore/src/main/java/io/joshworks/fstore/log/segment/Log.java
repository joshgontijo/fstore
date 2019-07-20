package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.Writer;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.Closeable;

public interface Log<T> extends Writer<T>, Closeable {

    long START = LogHeader.BYTES;
    byte[] EOL = new byte[Memory.PAGE_SIZE]; //eof header, -1 length, 0 crc

    long physicalSize();

    long logicalSize();

    long dataSize();

    long actualDataSize();

    long uncompressedDataSize();

    long headerSize();

    long footerSize();

    String name();

    SegmentIterator<T> iterator(long position, Direction direction);

    SegmentIterator<T> iterator(Direction direction);

    long position();

    T get(long position);

    long remaining();

    void delete();

    void roll(int level, boolean trim);

    boolean readOnly();

    boolean closed();

    long entries();

    int level();

    long created();

    long uncompressedSize();

    Type type();

}
