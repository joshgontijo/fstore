package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;

public interface BlockFactory {

    Block create(int blockSize);

    Block load(Codec codec, ByteBuffer data);

}
