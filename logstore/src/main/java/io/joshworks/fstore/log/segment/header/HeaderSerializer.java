package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

public class HeaderSerializer implements Serializer<LogHeader> {

    @Override
    public ByteBuffer toBytes(LogHeader data) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        writeTo(data, bb);
        return bb.flip();

    }

    // public final String magic;
    //    public final long created;
    //    public final Type type;
    //    public final long fileSize;
    //
    //    //completed info
    //    public final int level; //segments created are implicit level zero
    //    public final long entries;
    //    public final long logicalSize; //actual written bytes, including header
    //    public final long rolled;

    @Override
    public void writeTo(LogHeader data, ByteBuffer dest) {
        Serializers.VSTRING.writeTo(data.magic, dest);
        dest.putLong(data.created);
        dest.putInt(data.type.val);
        dest.putLong(data.fileSize);
        dest.putInt(data.encrypted ? 1 : 0);

        dest.putInt(data.level);
        dest.putLong(data.entries);
        dest.putLong(data.writePosition);
        dest.putLong(data.rolled);
        dest.putLong(data.uncompressedSize);
    }

    @Override
    public LogHeader fromBytes(ByteBuffer buffer) {
        String magic = Serializers.VSTRING.fromBytes(buffer);
        long created = buffer.getLong();
        int type = buffer.getInt();
        long fileSize = buffer.getLong();
        boolean encrypted = buffer.getInt() == 1;

        int level = buffer.getInt();
        long entries = buffer.getLong();
        long logicalSize = buffer.getLong();
        long rolled = buffer.getLong();
        long uncompressedSize = buffer.getLong();

        return new LogHeader(magic, entries, created, level, Type.of(type), rolled, fileSize, encrypted, logicalSize, uncompressedSize);
    }
}
