package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.es.SegmentDirectory;
import io.joshworks.es.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LogSegment implements SegmentFile {

    private static final Logger log = LoggerFactory.getLogger(LogSegment.class);

    private static final ThreadLocal<ByteBuffer> headerBuffer = ThreadLocal.withInitial(() -> Buffers.allocate(Header.BYTES, false));

    protected final File file;
    protected final SegmentChannel channel;

    private final Set<SegmentIterator> iterators = new HashSet<>();

    private final Header header = new Header();
    private final int idx;

    public static LogSegment create(File file, long initialSize) {
        LogSegment segment = new LogSegment(file);
        if (segment.readOnly()) {
            throw new IllegalStateException("Segment already exist");
        }
        segment.channel.truncate(initialSize);
        segment.channel.position(Header.BYTES);
        segment.header.created = System.currentTimeMillis();
        segment.writeHeader();
        return segment;
    }

    public static LogSegment open(File file) {
        LogSegment segment = new LogSegment(file);
        if (!segment.readOnly()) {
            segment.delete();
            throw new IllegalStateException("Segment does not exist");
        }
        return segment;
    }

    private LogSegment(File file) {
        this.file = file;
        this.channel = SegmentChannel.open(file);
        this.header.read();
        this.idx = SegmentDirectory.segmentIdx(this);
    }

    private void checkPosition(long pos) {
        if (pos < Header.BYTES) {
            throw new IllegalStateException("Position cannot be less than " + pos);
        }
    }

    public synchronized void restore(BufferPool pool) {
        log.info("Restoring {}", name());

        long start = System.currentTimeMillis();

        try (SegmentIterator it = iterator(pool)) {
            int processed = 0;

            long recordPos = Header.BYTES;
            while (it.hasNext()) {
                ByteBuffer record = it.next();
                onRecordRestored(record, recordPos);
                recordPos += Event.sizeOf(record);
                processed++;
            }
            long truncated = channel.position() - recordPos;
            channel.truncate(recordPos);

            header.entries.set(processed);

            log.info("Restored {}: {} entries in {}ms, truncated {} bytes, final segment size: {}", name(), processed, System.currentTimeMillis() - start, truncated, size());
        }
    }

    protected void onRecordRestored(ByteBuffer record, long recPos) {
        //do nothing
    }


    public int read(ByteBuffer dst, long position) {
        checkPosition(position);
        return Channels.read(channel, position, dst);
    }

    public long append(ByteBuffer record) {
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        try {
            long startPos = channel.position();
            Channels.writeFully(channel, record);
            header.entries.addAndGet(1);
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

        header.rolled = System.currentTimeMillis();
        writeHeader();
    }

    private void writeHeader() {
        boolean unmarked = channel.unmarkAsReadOnly();
        try {
            header.write();
        } finally {
            if (unmarked) {
                channel.markAsReadOnly();
            }
        }
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

    public SegmentIterator iterator(BufferPool pool) {
        return iterator(Header.BYTES, pool);
    }

    public synchronized SegmentIterator iterator(long pos, BufferPool pool) {
        checkPosition(pos);
        SegmentIterator it = new SegmentIterator(this, pos, pool);
        iterators.add(it);
        return it;
    }

    public synchronized void delete() {
        if (!header.markedForDeletion.compareAndSet(false, true)) {
            return;
        }
        writeHeader();
        if (!iterators.isEmpty()) {
            log.info("Segment marked for deletion");
            return;
        }
        doDelete();
    }

    @Override
    public File file() {
        return file;
    }

    protected void doDelete() {
        log.info("Deleting {}", name());
        channel.delete();
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
        return Channels.transferFully(channel, Header.BYTES, dst);
    }


    public long entries() {
        return header.entries.get();
    }

    protected synchronized void release(SegmentIterator iterator) {
        iterators.remove(iterator);
        if (header.markedForDeletion.get()) {
            synchronized (this) {
                doDelete();
            }
        }
    }

    @Override
    public String toString() {
        return "{" +
//                "level=" + level() +
//                ", idx=" + segmentIdx() +
                ", name=" + name() +
                ", writePosition=" + channel.position() +
                ", size=" + size() +
                ", entries=" + entries() +
                ", readOnly=" + readOnly() +
                ", Header=" + header +
                '}';
    }

    public int segmentIdx() {
        return idx;
    }

    /**
     * <pre>
     * ------ 4096 bytes -----
     * ENTRIES (8 bytes)
     * CREATED (8 bytes)
     * ROLLED (8 bytes)
     * MARKED_FOR_DELETION (1 byte)
     * </pre>
     */
    private class Header {

        private static final int START = 0;
        private static final int BYTES = 4096;

        private final AtomicLong entries = new AtomicLong();
        private long created;
        private long rolled;
        private final AtomicBoolean markedForDeletion = new AtomicBoolean();

        private void read() {
            ByteBuffer buffer = headerBuffer.get().clear();
            try {
                int read = channel.read(buffer, START);
                if (read < BYTES) {
                    return;
                }
                buffer.flip();

                entries.set(buffer.getLong());
                created = buffer.getLong();
                rolled = buffer.getLong();
                markedForDeletion.set(buffer.get() == 1);
            } catch (IOException e) {
                throw new RuntimeIOException("Failed to read header");
            }
        }

        private void write() {
            ByteBuffer buffer = headerBuffer.get().clear();
            try {
                buffer.putLong(entries.get());
                buffer.putLong(created);
                buffer.putLong(rolled);
                buffer.put((byte) (markedForDeletion.get() ? 1 : 0));

                buffer.flip();

                channel.write(buffer, START);

            } catch (IOException e) {
                throw new RuntimeIOException("Failed to read header");
            }
        }

        @Override
        public String toString() {
            return "{" +
                    "entries=" + entries.get() +
                    ", created=" + created +
                    ", rolled=" + rolled +
                    ", markedForDeletion=" + markedForDeletion.get() +
                    '}';
        }
    }

}
