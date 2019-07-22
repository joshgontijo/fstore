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

    /**
     * The underlying file size
     */
    long physicalSize();

    /**
     * The total bytes written in this segment, including header, data, EOL, and footer sections
     */
    long logicalSize();

    /**
     * Initial segment data section size
     */
    long dataSize();

    /**
     * Actual written bytes in this segment, can be greater than {@link Log#dataSize()}
     */
    long actualDataSize();

    /**
     * The total uncompressed size of the data section bytes, when no compression, this values is equal to {@link Log#actualDataSize()}
     */
    long uncompressedDataSize();

    /**
     * The size of the header, always {@link LogHeader#BYTES}
     */
    long headerSize();

    /**
     * The total of written bytes in the footer area
     */
    long footerSize();

    /**
     * Remaining bytes that can be written to the data size
     * @return
     */
    long remaining();

    String name();

    SegmentIterator<T> iterator(long position, Direction direction);

    SegmentIterator<T> iterator(Direction direction);

    long position();

    T get(long position);


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
