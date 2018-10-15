package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public interface BlockFactory<T, B extends Block<T>> {

    B create(Serializer<T> serializer, int maxBlockSize);

    B load(Serializer<T> serializer, Codec codec, ByteBuffer data);

}
