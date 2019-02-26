package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.Checksum;

import java.nio.ByteBuffer;

public class HeaderUtil {

    private static final int HEADER_SIZE = 1024; //each header can have at most 1024

    static <T extends Header> void write(Storage storage, T header, Serializer<T> serializer) {
        try {
            ByteBuffer headerData = serializer.toBytes(header);
            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(HEADER_SIZE);
            int entrySize = headerData.remaining();
            withChecksumAndLength.putInt(entrySize);
            withChecksumAndLength.putInt(Checksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            long prevPos = storage.writePosition();
            storage.writePosition(0);
            if (storage.write(withChecksumAndLength) != CompletedHeader.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            storage.writePosition(prevPos);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }

    static <T extends Header> T read(Storage storage, long position, Serializer<T> serializer) {
        if(storage.writePosition() < CreatedHeader.BYTES) {
            storage.writePosition(CreatedHeader.BYTES);
        }
        ByteBuffer bb = ByteBuffer.allocate(CreatedHeader.BYTES);
        storage.read(position, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return null;
        }
        int length = bb.getInt();
        if (length == 0) {
            return null;
        }
        int checksum = bb.getInt();
        bb.limit(bb.position() + length); //length + checksum
        if (Checksum.crc32(bb) != checksum) {
            throw new IllegalStateException("Log head checksum verification failed");
        }

        return serializer.fromBytes(bb);
    }


}
