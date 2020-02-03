package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.codec.Codec;

import java.nio.ByteBuffer;

public class CompressedBlockData {

    ByteBuffer backingBuffer;
    int offset;
    int count;

    public void decompress(ByteBuffer dst, Codec codec) {

    }


}
