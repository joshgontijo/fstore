package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Codec {

    /**
     * Compress a the remaining bytes in the given src into a dst
     * No size validation is made therefore, caller must ensure the destination will have enough space to accomodate the
     * compressed data, which in some cases can be slightly bigger than the uncompressed
     */
    void compress(ByteBuffer src, ByteBuffer dst);

    /**
     * Decompress the given source into the dst buffer.
     * Caller must ensure destination' limit is exactly the uncompressed size (LZ4).
     */
    void decompress(ByteBuffer src, ByteBuffer dst);

    static Codec noCompression() {
        return new Codec() {
            @Override
            public void compress(ByteBuffer src, ByteBuffer dst) {
                dst.put(src);
            }

            @Override
            public void decompress(ByteBuffer src, ByteBuffer dst) {
                dst.put(src);
            }
        };
    }

}
