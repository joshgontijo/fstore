package io.joshworks.fstore.log.record;

public class RecordEntry<T> {

    public static final int LENGTH_SIZE = Integer.BYTES; //length
    public static final int SECONDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads
    public static final int CHECKSUM_SIZE = Integer.BYTES; //crc32
    public static final int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    public static final int HEADER_OVERHEAD = MAIN_HEADER + LENGTH_SIZE; //length + crc32

    private final int size;
    private final long position;
    private final T entry;

    public RecordEntry(int size, T entry, long position) {
        this.size = size;
        this.entry = entry;
        this.position = position;
    }

    public static <T> RecordEntry<T> empty() {
        return new RecordEntry<>(0, null, -1);
    }

    public T entry() {
        return entry;
    }

    public int recordSize() {
        return size + RecordHeader.HEADER_OVERHEAD;
    }

    public int dataSize() {
        return size;
    }

    public long position() {
        return position;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
