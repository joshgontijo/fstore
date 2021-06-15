package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.WriteMode;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OpenSection {
    public final long created;
    public final long physical;
    public final long dataSize;
    public final boolean encrypted;
    public final WriteMode mode;

    public OpenSection(long created, WriteMode mode, long physical, long dataSize, boolean encrypted) {
        this.created = created;
        this.mode = mode;
        this.physical = physical;
        this.dataSize = dataSize;
        this.encrypted = encrypted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenSection that = (OpenSection) o;
        return created == that.created &&
                physical == that.physical &&
                dataSize == that.dataSize &&
                encrypted == that.encrypted &&
                mode == that.mode;
    }

    static Serializer<OpenSection> serializer() {
        return new Serializer<>() {
            @Override
            public void writeTo(OpenSection data, ByteBuffer dst) {
                dst.putLong(data.created);
                dst.putLong(data.physical);
                dst.putLong(data.dataSize);
                dst.put((byte) (data.encrypted ? 1 : 0));
                dst.putInt(data.mode.val);
            }

            @Override
            public OpenSection fromBytes(ByteBuffer buffer) {
                long created = buffer.getLong();
                long physical = buffer.getLong();
                long dataSize = buffer.getLong();
                boolean encrypted = buffer.get() == 1;
                WriteMode mode = WriteMode.of(buffer.getInt());
                return new OpenSection(created, mode, physical, dataSize, encrypted);
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, mode, physical, dataSize, encrypted);
    }

    @Override
    public String toString() {
        return "{" + "created=" + created +
                ", mode=" + mode +
                ", fileSize=" + physical +
                ", dataSize=" + dataSize +
                ", encrypted=" + encrypted +
                '}';
    }
}
