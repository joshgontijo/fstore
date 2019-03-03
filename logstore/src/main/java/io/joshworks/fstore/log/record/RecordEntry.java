package io.joshworks.fstore.log.record;

public class RecordEntry<T> {

    private final int size;
    private final T entry;

    public RecordEntry(int size, T entry) {
        this.size = size;
        this.entry = entry;
    }

    public T entry() {
        return entry;
    }

    public int recordSize() {
        return dataSize() + RecordHeader.HEADER_OVERHEAD;
    }

    public int dataSize() {
        return size;
    }
}
