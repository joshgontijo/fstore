package io.joshworks;

import java.nio.ByteBuffer;

public class Record {
    public final long sequence;
    public final ByteBuffer data;

    Record(long sequence, ByteBuffer data) {
        this.sequence = sequence;
        this.data = data;
    }
}
