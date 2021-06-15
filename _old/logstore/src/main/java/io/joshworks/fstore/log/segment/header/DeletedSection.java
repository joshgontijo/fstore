package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class DeletedSection {
    public final long timestamp;

    public DeletedSection(long timestamp) {
        this.timestamp = timestamp;
    }

    static Serializer<DeletedSection> serializer() {
        return new Serializer<>() {
            @Override
            public void writeTo(DeletedSection data, ByteBuffer dst) {
                dst.putLong(data.timestamp);
            }

            @Override
            public DeletedSection fromBytes(ByteBuffer buffer) {
                long timestamp = buffer.getLong();
                return new DeletedSection(timestamp);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletedSection that = (DeletedSection) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp);
    }

    @Override
    public String toString() {
        return "{" +
                "timestamp=" + timestamp +
                '}';
    }
}
