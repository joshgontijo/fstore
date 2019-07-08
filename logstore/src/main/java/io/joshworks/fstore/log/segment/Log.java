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

    String name();

    SegmentIterator<T> iterator(long position, Direction direction);

    SegmentIterator<T> iterator(Direction direction);

    long position();

    T get(long position);

    long fileSize();

    long logSize();

    long remaining();

    void delete();

    void roll(int level);

    boolean readOnly();

    boolean closed();

    long entries();

    int level();

    long created();

    void trim();

    long uncompressedSize();

    Type type();

//    SegmentState rebuildState(long lastKnownPosition);
//
//    /**
//     * Used for reloading segment on startup, return type must be number of entries.
//     * For a default segment the return value would be the actual list size,
//     * while in a block segment, the implementation would return the total number of entries in each block
//     * This method can also be useful to load entries into the index when the storage is brought back up.
//     * @param items The collection of items read in a single bulk read.
//     * @return The actual number of records
//     */
//    long processEntries(List<RecordEntry<T>> items);

}
