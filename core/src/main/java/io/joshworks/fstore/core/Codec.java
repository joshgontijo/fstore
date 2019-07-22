package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Codec {

    /**
     * Compress a the remaining bytes in the given src into a dst
     * No size validation is made therefore,      *
     *
     * @param src
     * @param dst
     */
    void compress(ByteBuffer src, ByteBuffer dst);

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
