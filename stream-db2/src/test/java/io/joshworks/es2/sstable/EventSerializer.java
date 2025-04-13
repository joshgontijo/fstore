package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.StreamHasher;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.joshworks.fstore.core.util.StringUtils.toUtf8Bytes;

public class EventSerializer {

    public static ByteBuffer serialize(String stream, String type, int version, String data) {
        byte[] dataBytes = toUtf8Bytes(data);
        byte[] typeBytes = toUtf8Bytes(type);
        int recSize = dataBytes.length + typeBytes.length + Event.HEADER_BYTES;

        long streamHash = StreamHasher.hash(stream);

        ByteBuffer dst = ByteBuffer.allocate(recSize);
        int bpos = dst.position();

        //FIXME missing checksum and other fields, was this a unfixed change to the event ?
        dst.putInt(recSize);
        dst.putLong(streamHash);
        dst.putInt(version);
        dst.putLong(System.currentTimeMillis());

        dst.putShort((short) typeBytes.length);
        dst.putInt(dataBytes.length);

        dst.put(typeBytes);
        dst.put(dataBytes);

        int copied = (dst.position() - bpos);
        assert copied == recSize;

        return dst.flip();
    }

}
