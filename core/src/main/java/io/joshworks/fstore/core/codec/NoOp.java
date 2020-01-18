package io.joshworks.fstore.core.codec;

import java.nio.ByteBuffer;

class NoOp implements Codec {

    static Codec INSTANCE = new NoOp();

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        dst.put(src);
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        dst.put(src);
    }
}
