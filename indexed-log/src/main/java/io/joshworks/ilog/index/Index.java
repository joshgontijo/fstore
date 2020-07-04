package io.joshworks.ilog.index;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.util.Objects.requireNonNull;

/**
 * A NON-CLUSTERED, UNIQUE, ORDERED index that uses binary search to read elements
 * - Append only
 * - Entries must be of a fixed size
 * - Insertion must be ORDERED
 * - Entries must be unique
 * <p>
 * If opening from an existing file, the index is marked as read only.
 * <p>
 * ENTRY FORMAT:
 * KEY (N bytes)
 * LOG_POS (8 bytes)
 * ENTRY_SIZE (4 bytes)
 */
public class Index implements Closeable {

    private final MappedRegion region;
    private final RowKey comparator;
    private final Header header;
    public static final int NONE = -1;

    public Index(FileChannel channel, long start, int indexSize, RowKey comparator) {
        indexSize = align(indexSize);
        tryResizeChannel(channel, Header.BYTES + indexSize);

        this.comparator = comparator;
        this.header = new Header(channel, start);
        long indexStart = start + Header.BYTES;

        this.region = new MappedRegion(channel, indexStart, indexSize);
        if (header.completed()) {
            openIndex(comparator, header, indexSize);
        }
    }

    private void tryResizeChannel(FileChannel channel, int regionSize) {
        try {
            if (channel.size() < regionSize) {
                channel.truncate(regionSize);
            }
        } catch (Exception e) {
            throw new RuntimeIOException("Could not extend channel to accomodate index");
        }
    }

    private void openIndex(RowKey comparator, Header header, int indexSize) {
        int storedIndexSize = header.indexSize();
        int entries = header.entries();
        int maxEntries = indexSize / entrySize();

        assert storedIndexSize == indexSize;
        assert entries >= 0;
        assert entries <= maxEntries;
        assert header.keySize() == comparator.keySize();

        int indexPos = entries * entrySize();

        region.position(indexPos); //all information is derived from buffer pos and entry size
    }

    public void write(ByteBuffer src, int recordSize, long position) {
        write(src, src.position(), src.limit(), recordSize, position);
    }

    /**
     * Writes an entry to this index
     *
     * @param src      The source buffer to get the key from
     * @param kOffset  The key offset in the source buffer
     * @param kCount   The size of the key, must match Rowkey#keySize()
     * @param position The entry position in the log
     */
    public void write(ByteBuffer src, int kOffset, int kCount, int recordSize, long position) {
        if (isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (kCount != comparator.keySize()) {
            throw new RuntimeException("Invalid index key length, expected " + comparator.keySize() + ", got " + kCount);
        }
        region.put(src, kOffset, kCount);
        region.putLong(position);
        region.putInt(recordSize);
    }

    /**
     * Returns the index of the given key by applying the given {@link IndexFunction}
     */
    public int find(ByteBuffer key, IndexFunction func) {
        requireNonNull(key, "Key must be provided");
        int remaining = key.remaining();
        int kSize = keySize();
        if (remaining != kSize) {
            throw new IllegalArgumentException("Invalid key size: " + remaining + ", expected: " + kSize);
        }
        if (entries() == 0) {
            return NONE;
        }
        int idx = binarySearch(key);
        return func.apply(idx);
    }

    private int binarySearch(ByteBuffer key) {
        return binarySearch(key, region.buffer(), 0, indexSize(), entrySize(), comparator);
    }

    private static int binarySearch(ByteBuffer key, ByteBuffer data, int dataStart, int dataCount, int entrySize, RowKey comparator) {
        if (dataCount % entrySize != 0) {
            throw new IllegalArgumentException("Read buffer must be multiple of " + entrySize);
        }
        int entries = dataCount / entrySize;

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = dataStart + (mid * entrySize);
            if (readPos < dataStart || readPos > dataStart + dataCount) {
                throw new IndexOutOfBoundsException("Index out of bounds: " + readPos);
            }
            int cmp = comparator.compare(data, readPos, key, key.position());
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }


    public long readPosition(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize();
        return region.getLong(positionOffset);
    }

    public int readEntrySize(int idx) {
        if (idx < 0 || idx >= entries()) {
            return NONE;
        }
        int startPos = idx * entrySize();
        int positionOffset = startPos + comparator.keySize() + Long.BYTES;
        return region.getInt(positionOffset);
    }

    public boolean isFull() {
        return remaining() == 0;
    }

    public int entries() {
        return (region.position() / entrySize());
    }

    public void delete() {
        region.clear();
    }

    public boolean readOnly() {
        return header.completed();
    }

    public void flush() {
        region.flush();
    }

    @Override
    public void close() {
        region.close();
    }

    protected int entrySize() {
        return keySize() + Long.BYTES + Integer.BYTES;
    }

    public int keySize() {
        return comparator.keySize();
    }

    /**
     * Complete this index and mark it as read only.
     */
    public void complete() {
        header.complete();
    }

    /**
     * Actual index size, including HEADER
     */
    public int size() {
        return Header.BYTES + indexSize();
    }

    /**
     * Actual index size, excluding HEADER
     */
    private int indexSize() {
        return entries() * entrySize();
    }

    public void first(ByteBuffer dst) {
        if (entries() == 0) {
            return;
        }
        if (dst.remaining() != keySize()) {
            throw new RuntimeException("Buffer key length mismatch");
        }
        region.get(dst, 0, keySize());
    }

    public int remaining() {
        return region.capacity() / entrySize();
    }

    private int align(int size) {
        int entrySize = entrySize();
        int aligned = entrySize * (size / entrySize);
        if (aligned <= 0 || aligned > size) {
            throw new IllegalArgumentException("Invalid index size: " + size);
        }
        return aligned;
    }

    private class Header {

        private static final int BYTES = (Integer.BYTES * 4) + Byte.BYTES;

        private final ByteBuffer buffer = Buffers.allocate(BYTES, false);
        private final FileChannel channel;
        private final long position;

        private static final int INDEX_CAPACITY = 0;
        private static final int COMPLETED_FLAG = INDEX_CAPACITY + Integer.BYTES;
        private static final int ENTRIES = COMPLETED_FLAG + Byte.BYTES;
        private static final int INDEX_SIZE = ENTRIES + Integer.BYTES;
        private static final int KEY_SIZE = INDEX_SIZE + Integer.BYTES;

        private Header(FileChannel channel, long position) {
            this.channel = channel;
            this.position = position;
            try {
                int read = channel.read(buffer, position);
                if (read < buffer.capacity()) {
                    throw new IllegalStateException("Invalid header size");
                }
                buffer.flip();
            } catch (Exception e) {
                throw new RuntimeIOException("Failed to read index header", e);
            }
        }

        public void complete() {
            buffer.clear();
            buffer.putInt(INDEX_CAPACITY, region.capacity()); //INDEX_CAPACITY
            buffer.put(COMPLETED_FLAG, (byte) 1); //COMPLETED_FLAG
            buffer.putInt(ENTRIES, entries()); //ENTRIES
            buffer.putInt(INDEX_SIZE, indexSize()); //INDEX_SIZE
            buffer.putInt(KEY_SIZE, comparator.keySize()); //KEY_SIZE
            try {
                int written = channel.write(buffer, position);
                if (written != BYTES) {
                    throw new IllegalStateException("Expected bytes written: " + BYTES + ", got: " + written);
                }
            } catch (Exception e) {
                throw new RuntimeIOException("Failed to write index header");
            }
        }

        private int indexSize() {
            return buffer.getInt(INDEX_CAPACITY);
        }

        private boolean completed() {
            return buffer.get(COMPLETED_FLAG) == 1;
        }

        private int indexUtilized() {
            return buffer.getInt(INDEX_SIZE);
        }

        private int keySize() {
            return buffer.getInt(KEY_SIZE);
        }

        private int entries() {
            return buffer.getInt(ENTRIES);
        }

    }

}