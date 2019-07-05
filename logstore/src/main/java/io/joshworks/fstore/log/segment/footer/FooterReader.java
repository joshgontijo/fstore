package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.segment.header.LogHeader;

public class FooterReader {

    private final DataStream stream;
    private final LogHeader header;

    private long currPos;

    public FooterReader(DataStream stream, LogHeader header) {
        this.stream = stream;
        this.header = header;
    }

    public <T> T read(Serializer<T> serializer) {
        RecordEntry<T> entry = readInternal(currPos, serializer);
        if (entry == null) {
            return null;
        }
        currPos += currPos + entry.recordSize();
        return entry.entry();
    }

    public <T> T read(long pos, Serializer<T> serializer) {
        RecordEntry<T> entry = readInternal(pos, serializer);
        if (entry == null) {
            return null;
        }
        return entry.entry();
    }

    private <T> RecordEntry<T> readInternal(long pos, Serializer<T> serializer) {
        checkReadOnly();
        if (!withinBounds(pos)) {
            return null;
        }
        return stream.read(Direction.FORWARD, pos, serializer);
    }

    public long start() {
        checkReadOnly();
        return header.footerLength();
    }

    public long length() {
        checkReadOnly();
        return stream.length();
    }

    public long position() {
        checkReadOnly();
        return stream.position();
    }

    public void position(long position) {
        checkReadOnly();
        if (!withinBounds(position)) {
            long start = header.footerStart();
            throw new IllegalStateException("Invalid footer position: " + position + ", allowed range is: " + start + " - " + maxPos());
        }
        this.currPos = position;
    }

    private void checkReadOnly() {
        if (!header.readOnly()) {
            throw new IllegalStateException("Segment is not readonly");
        }
    }

    private boolean withinBounds(long position) {
        return position >= header.footerStart() && position <= maxPos();
    }

    private long maxPos() {
        return header.footerStart() + header.footerLength() - 1;
    }

}
