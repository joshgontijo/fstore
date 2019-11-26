package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class MergeSection {
    public final long timestamp;

    public MergeSection(long timestamp) {
        this.timestamp = timestamp;
    }

    static Serializer<MergeSection> serializer() {
        return new Serializer<>() {
            @Override
            public void writeTo(MergeSection data, ByteBuffer dst) {
                dst.putLong(data.timestamp);
            }

            @Override
            public MergeSection fromBytes(ByteBuffer buffer) {
                long timestamp = buffer.getLong();
                return new MergeSection(timestamp);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeSection that = (MergeSection) o;
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
