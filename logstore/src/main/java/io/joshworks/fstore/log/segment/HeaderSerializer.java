package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

public class HeaderSerializer implements Serializer<LogHeader> {

    @Override
    public ByteBuffer toBytes(LogHeader data) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        writeTo(data, bb);
        return (ByteBuffer) bb.position(0); //do not flip, the header will always have the fixed size

    }

    @Override
    public void writeTo(LogHeader data, ByteBuffer dest) {


        dest.putLong(data.created);
        dest.putInt(data.type.val);
        Serializers.VSTRING.writeTo(data.magic, dest);
        dest.putInt(data.level);
        dest.putLong(data.segmentSize);

        dest.putLong(data.logStart);
        dest.putLong(data.logEnd);
        dest.putLong(data.entries);

        dest.putLong(data.footerStart);
        dest.putLong(data.footerEnd);
    }

    @Override
    public LogHeader fromBytes(ByteBuffer buffer) {
        if (buffer.remaining() != LogHeader.BYTES) {
            throw new IllegalStateException("Expected " + LogHeader.BYTES + " header length");
        }

        long created = buffer.getLong();
        int type = buffer.getInt();
        if(created == 0 || type == 0) { //empty
            return LogHeader.noHeader();
        }
        String magic = Serializers.VSTRING.fromBytes(buffer);
        int level = buffer.getInt();
        long segmentSize = buffer.getLong();

        long logStart = buffer.getLong();
        long logEnd = buffer.getLong();
        long entries = buffer.getLong();

        long footerStart = buffer.getLong();
        long footerEnd = buffer.getLong();

        return LogHeader.create(magic, entries, created, level, Type.of(type), segmentSize, logStart, logEnd, footerStart, footerEnd);

    }
}
