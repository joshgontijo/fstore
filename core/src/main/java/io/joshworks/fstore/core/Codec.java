package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

//------- UNCOMPRESSED HEADER -------
//UNCOMPRESSED_SIZE (4bytes)
//------- COMPRESSED ---------
//DATA (N bytes)
public interface Codec {

    int BLOCK_HEADER_SIZE = Integer.BYTES;

    ByteBuffer compress(ByteBuffer data);

    ByteBuffer decompress(ByteBuffer compressed);

    static Codec noCompression() {
        return new Codec() {
            @Override
            public ByteBuffer compress(ByteBuffer data) {
                int totalSize = BLOCK_HEADER_SIZE + data.remaining();
                var bb = data.isDirect() ? ByteBuffer.allocateDirect(totalSize) : ByteBuffer.allocate(totalSize);
                //we need to add for consistency with other compression methods, additional buffer copy here
                bb.putInt(data.remaining());
                bb.put(data);
                return bb.flip();
            }

            @Override
            public ByteBuffer decompress(ByteBuffer compressed) {
                compressed.position(compressed.position() + BLOCK_HEADER_SIZE);
                return compressed.slice();
            }
        };
    }

}
