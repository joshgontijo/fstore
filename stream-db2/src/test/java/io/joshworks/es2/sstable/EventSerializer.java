package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.StreamHasher;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EventSerializer {

    public static ByteBuffer serialize(String stream, String type, int version, String data, long sequence) {
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
        int checksum = ByteBufferChecksum.crc32(ByteBuffer.wrap(dataBytes));
        int recSize = dataBytes.length + typeBytes.length + Event.HEADER_BYTES;

        long streamHash = StreamHasher.hash(stream);

        ByteBuffer dst = ByteBuffer.allocate(recSize);
        int bpos = dst.position();

        byte[] evTypeBytes = StringUtils.toUtf8Bytes(type);
        dst.putInt(recSize);
        dst.putLong(streamHash);
        dst.putInt(version);
        dst.putInt(checksum); //tmp checksum
        dst.putLong(sequence);
        dst.putLong(System.currentTimeMillis());

        dst.putShort((short) evTypeBytes.length);
        dst.putInt(dataBytes.length);

        dst.put(evTypeBytes);
        dst.put(dataBytes);

        int copied = (dst.position() - bpos);

        assert copied == recSize;
        dst.flip();
        assert Event.isValid(dst);
        return dst;
    }

//    public static ByteBuffer serialize(String stream, String type, int version, String data, long sequence) {
//        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
//        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
//        int checksum = ByteBufferChecksum.crc32(ByteBuffer.wrap(dataBytes));
//        int recSize = dataBytes.length + typeBytes.length + Event.HEADER_BYTES;
//
//        long streamHash = StreamHasher.hash(stream);
//
//        ByteBuffer dst = ByteBuffer.allocate(recSize);
//        int bpos = dst.position();
//
//        byte[] evTypeBytes = StringUtils.toUtf8Bytes(type);
//        dst.putInt(recSize);
//        dst.putLong(streamHash);
//        dst.putInt(version);
//        dst.putInt(checksum); //tmp checksum
//        dst.putLong(sequence);
//        dst.putLong(System.currentTimeMillis());
//
//        dst.putShort((short) evTypeBytes.length);
//        dst.putInt(dataBytes.length);
//
//        dst.put(evTypeBytes);
//        dst.put(dataBytes);
//
//        int copied = (dst.position() - bpos);
//
//        assert copied == recSize;
//        assert Event.isValid(dst, bpos);
//        return dst.flip();
//    }


}
