package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.List;

public interface Block {

    boolean add(ByteBuffer data);

    ByteBuffer pack(Codec codec);

    int entryCount();

    List<ByteBuffer> entries();

    <T> List<T> deserialize(Serializer<T> serializer);

    ByteBuffer get(int pos);

    ByteBuffer first();

    ByteBuffer last();

    boolean readOnly();

    boolean isEmpty();

    long uncompressedSize();

    List<Integer> entriesLength();


}
