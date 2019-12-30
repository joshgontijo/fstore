package io.joshworks;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class Record {
    public final long sequence;
    public final ByteBuffer data;

    Record(long sequence, ByteBuffer data) {
        this.sequence = sequence;
        this.data = data;
    }

    public <T> T read(Serializer<T> serializer) {
        return serializer.fromBytes(data);
    }

}
