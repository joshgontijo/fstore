package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

public class BufferRecords extends AbstractRecords {

    //the active records
    private final Queue<Record2> records = new ArrayDeque<>();

    //the active record buffers
    private final Queue<ByteBuffer> buffers = new ArrayDeque<>();

    //the object cache where all instances should return to
    private final Queue<Record2> cache = new ArrayDeque<>();

    //temp holder for buffers so it can gather write to a channel
    private final ByteBuffer[] tmp;

    protected final RowKey rowKey;
    private final int maxItems;

    BufferRecords(RecordPool pool, RowKey rowKey, int maxItems) {
        super(pool);
        this.tmp = new ByteBuffer[maxItems];
        this.rowKey = rowKey;
        this.maxItems = maxItems;
    }

    //copied the record into this pool
    public boolean add(Record2 record) {
        if (records.size() >= maxItems) {
            return false;
        }
        ByteBuffer recData = pool.allocate(record.recordSize());
        int copied = record.copyTo(recData);
        if (copied > 0) {
            recData.flip();
            add(recData);
        }
        return true;
    }

    void add(ByteBuffer data) {
        if (!RecordUtils.isValid(data)) {
            pool.free(data);
            throw new RuntimeException("Invalid record"); // should never happen
        }

        Record2 record = allocateEmptyRecord();
        record.data = data;
        int recSize = data.remaining();

        records.add(record);
        buffers.add(data);
    }

    //TODO MOVE TO SOMEWHERE ELSE
    public void create(long sequence, ByteBuffer value, Consumer<ByteBuffer> keyWriter) {
        int rsize = Record2.HEADER_BYTES + Integer.BYTES + value.remaining();
        ByteBuffer recdata = pool.allocate(rsize);
        recdata.position(Record2.KEY_OFFSET);
        keyWriter.accept(recdata);
        if (recdata.position() - Record2.KEY_OFFSET != rowKey.keySize()) {
            throw new IllegalStateException("Invalid row key size");
        }
        int checksum = ByteBufferChecksum.crc32(value);
        int valLen = value.remaining();
        Buffers.copy(value, recdata);

        recdata.position(0);
        writeHeader(recdata, sequence, rowKey.keySize(), valLen, checksum);

        add(recdata);
    }

    private static void writeHeader(ByteBuffer dst, long sequence, int keyLen, int valueLen, int checksum) {
        int recLen = Record2.HEADER_BYTES + keyLen + valueLen;

        int ppos = dst.position();

        dst.putInt(recLen); // RECORD_LEN
        dst.putInt(valueLen); // VALUE_LEN
        dst.putLong(sequence); // SEQUENCE
        dst.putInt(checksum); // CHECKSUM
        dst.putLong(System.currentTimeMillis()); // TIMESTAMP
        dst.put((byte) 0); // ATTRIBUTES

        dst.limit(ppos + recLen).position(ppos);
    }

    @Override
    public Record2 poll() {
        Record2 poll = records.poll();
        if (poll == null) {
            return null;
        }
        ByteBuffer buff = buffers.poll();
        if (poll.data != buff) {
            throw new IllegalStateException("Invalid buffer queue"); // should never happen
        }
        return poll;
    }

    @Override
    public Record2 peek() {
        return records.peek();
    }

    public boolean remove() {
        Record2 rec = records.poll();
        if (rec == null) {
            return false;
        }
        free(rec);

        return true;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    private Record2 allocateEmptyRecord() {
        Record2 poll = cache.poll();
        return poll == null ? new Record2(this, rowKey) : poll;
    }

    @Override
    public void close() {
        clear();
        pool.free(this);
        records.clear();
    }

    void free(Record2 record) {
        if (record.owner != this) {
            throw new IllegalArgumentException("Record pool not owner of this record");
        }

        Record2 first = records.peek();
        //record was peeked and closed, remove from queue
        if (first != null && first.equals(record)) {
            records.poll();
        }

        pool.free(record.data);
        record.data = null;
        cache.add(record);
    }

    public int size() {
        return records.size();
    }

    public boolean isFull() {
        return records.size() >= maxItems;
    }

    public void clear() {
        while (hasNext()) {
            remove();
        }
    }

    @Override
    public long writeTo(IndexedSegment segment) {
        if (segment.index().isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (segment.readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (records.size() == 0) {
            return 0;
        }

        try {
            FileChannel channel = segment.channel();
            Index index = segment.index();

            long totalWritten = 0;

            long recordPos = channel.position();
            while (hasNext() && !index.isFull()) {
                int count = Math.min(index.remaining(), records.size());
                buffers.toArray(tmp);
                long written = Buffers.writeFully(channel, tmp, 0, count);
                checkClosed(written);
                totalWritten += written;

                //files are always available, no need to use removeWrittenEntries
                //poll entries and add to index
                for (int i = 0; i < count; i++) {
                    try (Record2 rec = poll()) {
                        recordPos += rec.writeToIndex(index, recordPos);
                    }
                }
            }
            return totalWritten;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to segment");
        }
    }

    private void checkClosed(long w) {
        if (w == -1) {
            throw new RuntimeIOException("Channel closed");
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        return writeTo(channel, buffers.size());
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        try {
            long totalWritten = 0;
            while (hasNext()) {
                buffers.toArray(tmp);
                count = Math.min(count, buffers.size());
                long written = Buffers.writeFully(channel, tmp, 0, count);
                checkClosed(written);
                totalWritten += written;

                removeWrittenEntries(written);
            }
            return totalWritten;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to channel", e);
        }
    }

    private void removeWrittenEntries(long written) {
        //removes only fully written entries, leftovers will be retried next iteration
        int bytesRemoved = 0;
        while (bytesRemoved < written) {
            Record2 rec = peek();
            int recordSize = rec.recordSize();
            if (bytesRemoved + recordSize <= written) {
                remove();
                bytesRemoved += recordSize;
            }
        }
    }

    public Records copy() {
        BufferRecords copy = pool.getBufferRecords();
        for (Record2 record : records) {
            copy.add(record);
        }
        return copy;
    }
}
