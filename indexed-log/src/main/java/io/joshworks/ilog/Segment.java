package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Channels;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
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

public class Segment implements Iterable<Record> {

    private static final Logger log = LoggerFactory.getLogger(Segment.class);

    public static final int NO_MAX_SIZE = -1;
    public static final int NO_MAX_ENTRIES = -1;

    protected final File file;
    protected final long maxSize;
    protected final long maxEntries;
    protected final RecordPool pool;
    protected final SegmentChannel channel;

    private final Set<SegmentIterator> iterators = new HashSet<>();

    private final Header header = new Header();

    public Segment(File file, RecordPool pool, long maxSize, long maxEntries) {
        this.file = file;
        this.maxSize = maxSize;
        this.maxEntries = maxEntries;
        this.pool = pool;
        this.channel = SegmentChannel.open(file);
        this.header.read();
        if (!channel.readOnly()) {
            this.channel.position(Header.BYTES);
            this.header.created = System.currentTimeMillis();
            writeHeader();
        }
    }


    private void checkPosition(long pos) {
        if (pos < Header.BYTES) {
            throw new IllegalStateException("Position cannot be less than " + pos);
        }
    }

    public synchronized void restore() {
        log.info("Restoring {}", name());

        long start = System.currentTimeMillis();

        try (SegmentIterator it = iterator()) {
            int processed = 0;

            long recordPos = Header.BYTES;
            while (it.hasNext()) {
                Record record = it.next();
                onRecordRestored(record, recordPos);
                recordPos += record.recordSize();
                processed++;
            }
            long truncated = channel.position() - recordPos;
            channel.truncate(recordPos);

            header.entries.set(processed);

            log.info("Restored {}: {} entries in {}ms, truncated {} bytes, final segment size: {}", name(), processed, System.currentTimeMillis() - start, truncated, size());
        }
    }

    protected void onRecordRestored(Record record, long recPos) {
        //do nothing
    }


    int read(ByteBuffer dst, long position) {
        checkPosition(position);
        return Channels.read(channel, position, dst);
    }

    //return the number of written records
    public int append(Records records, int offset) {
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (records.isEmpty()) {
            return 0;
        }

        int count = records.size() - offset;
        append(records, offset, count);
        return count;
    }

    protected long append(Records records, int offset, int count) {
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (records.isEmpty()) {
            return 0;
        }
        long startPos = channel.position();
        records.writeTo(channel, offset, count);
        header.entries.addAndGet(count);
        return startPos;
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

    public boolean isFull() {
        return (maxSize != NO_MAX_SIZE && size() > maxSize) || (maxEntries != NO_MAX_ENTRIES && entries() >= maxEntries);
    }

    public int level() {
        return LogUtil.levelOf(name());
    }

    public long segmentIdx() {
        return LogUtil.segmentIdx(name());
    }

    @Override
    public SegmentIterator iterator() {
        return iterator(Header.BYTES);
    }

    public synchronized SegmentIterator iterator(long pos) {
        checkPosition(pos);
        SegmentIterator it = new SegmentIterator(this, pos, Size.KB.ofInt(4), pool);
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

    public long transferTo(Segment dst) {
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
                "level=" + level() +
                ", idx=" + segmentIdx() +
                ", name=" + name() +
                ", writePosition=" + channel.position() +
                ", size=" + size() +
                ", entries=" + entries() +
                ", readOnly=" + readOnly() +
                ", Header=" + header +
                '}';
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
            ByteBuffer buffer = pool.allocate(BYTES);
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
            } finally {
                pool.free(buffer);
            }
        }

        private void write() {
            ByteBuffer buffer = pool.allocate(BYTES);
            try {
                buffer.putLong(entries.get());
                buffer.putLong(created);
                buffer.putLong(rolled);
                buffer.put((byte) (markedForDeletion.get() ? 1 : 0));

                buffer.flip();

                channel.write(buffer, START);

            } catch (IOException e) {
                throw new RuntimeIOException("Failed to read header");
            } finally {
                pool.free(buffer);
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
