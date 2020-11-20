package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.util.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.Function;
import java.util.function.Predicate;

public class LogSegment implements SegmentFile {

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    protected final File file;
    protected final SegmentChannel channel;

    private final long idx;
    private final int level;

    public static LogSegment create(File file, long initialSize) {
        SegmentChannel channel = SegmentChannel.create(file, initialSize);
        LogSegment segment = new LogSegment(file, channel);
        if (segment.readOnly()) {
            throw new IllegalStateException("Segment already exist");
        }
        segment.channel.position(Log.START);
        return segment;
    }

    public static LogSegment open(File file) {
        SegmentChannel channel = SegmentChannel.open(file);
        LogSegment segment = new LogSegment(file, channel);
        if (!segment.readOnly()) {
            segment.delete();
            throw new IllegalStateException("Segment does not exist");
        }
        return segment;
    }

    private LogSegment(File file, SegmentChannel channel) {
        this.file = file;
        this.channel = channel;
        this.idx = SegmentDirectory.segmentIdx(this);
        this.level = SegmentDirectory.level(this);
    }

    public synchronized long restore(Predicate<ByteBuffer> onRecord) {
        log.info("Restoring {}", name());

        long start = System.currentTimeMillis();

        try (SegmentIterator it = iterator()) {
            int processed = 0;

            long recordPos = Log.START;
            boolean valid = true;
            while (it.hasNext() && valid) {
                ByteBuffer record = it.next();
                valid = onRecord.test(record);
                if (valid) {
                    recordPos += Event.sizeOf(record);
                    processed++;
                } else {
                    log.warn("Found invalid record at {}, trimming log", recordPos);
                }
            }
            long truncated = channel.position() - recordPos;
            channel.truncate(recordPos);

            log.info("Restored {}: {} entries in {}ms, truncated {} bytes, final segment size: {}", name(), processed, System.currentTimeMillis() - start, truncated, size());
            return processed;
        }
    }

    public int read(ByteBuffer dst, long position) {
        checkPosition(position);
        return Channels.read(channel, position, dst);
    }

    public long append(ByteBuffer records) {
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        try {
            long startPos = channel.position();
            Channels.writeFully(channel, records);
            return startPos;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to append to log", e);
        }
    }


    public boolean readOnly() {
        return channel.readOnly();
    }

    long writePosition() {
        return channel.position();
    }

    void forceRoll() {
        flush();
        channel.truncate();
    }

    public void roll() {
        if (!channel.markAsReadOnly()) {
            throw new IllegalStateException("Already read only: " + name());
        }
        forceRoll();
    }

    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void flush() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to flush segment", e);
        }
    }

    public String name() {
        return channel.name();
    }

    public long segmentIdx() {
        return idx;
    }

    public int level() {
        return level;
    }

    public SegmentIterator iterator() {
        return iterator(Memory.PAGE_SIZE);
    }

    public SegmentIterator iterator(int bufferSize) {
        return iterator(bufferSize, Log.START);
    }

    public synchronized SegmentIterator iterator(int bufferSize, long pos) {
        checkPosition(pos);
        return new SegmentIterator(this, pos, bufferSize);
    }

    public synchronized void delete() {
        log.info("Deleting {}", name());
        channel.delete();
    }

    @Override
    public File file() {
        return file;
    }

    public void close() {
        try {
            channel.close();
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to close segment", e);
        }
    }

    public long transferTo(LogSegment dst) {
        return transferTo(dst.channel);
    }

    public long transferTo(WritableByteChannel dst) {
        return Channels.transferFully(channel, Log.START, dst);
    }

    private void checkPosition(long position) {
        long writePos = writePosition();
        if (position > writePos) {
            throw new IllegalArgumentException("Position must be less than " + writePos + ", got " + position);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "level=" + level() +
                ", idx=" + segmentIdx() +
                ", name=" + name() +
                ", writePosition=" + channel.position() +
                ", size=" + size() +
                ", readOnly=" + readOnly() +
                '}';
    }


}
