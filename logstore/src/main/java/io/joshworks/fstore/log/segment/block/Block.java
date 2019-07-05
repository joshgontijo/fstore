package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.List;

public interface Block {

    boolean add(ByteBuffer data);

    /**
     * Must return a ByteBuffer containing at least 8 bytes header:
     * UNCOMPRESED_SIZE
     * ENTRY_COUNT
     */
    ByteBuffer pack(Codec codec);

    int entryCount();

    List<ByteBuffer> entries();

    <T> List<T> deserialize(Serializer<T> serializer);

    ByteBuffer get(int pos);

    ByteBuffer first();

    ByteBuffer last();

    boolean readOnly();

    boolean isEmpty();

    int uncompressedSize();

    List<Integer> entriesLength();

    static int uncompressedSize(ByteBuffer compressed) {
        return compressed.getInt(compressed.position());
    }

    static int entries(ByteBuffer compressed) {
        return compressed.getInt(compressed.position() + Integer.BYTES);
    }

}
