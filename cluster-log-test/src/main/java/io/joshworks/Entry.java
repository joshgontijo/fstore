package io.joshworks;

public class Entry<T> {
    public final long sequence;
    public final T data;

    Entry(long sequence, T data) {
        this.sequence = sequence;
        this.data = data;
    }
}
