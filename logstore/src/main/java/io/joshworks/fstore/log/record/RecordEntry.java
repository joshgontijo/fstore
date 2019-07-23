package io.joshworks.fstore.log.record;

public class RecordEntry<T> {

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
