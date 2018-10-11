package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public interface BlockFactory<T> {

    Block<T> create(Serializer<T> serializer, int maxBlockSize);

    Block<T> load(Serializer<T> serializer, Codec codec, ByteBuffer data);

}
