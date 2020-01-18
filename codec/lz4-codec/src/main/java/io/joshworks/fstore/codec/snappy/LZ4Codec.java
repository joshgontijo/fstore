package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.codec.Codec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;

public class LZ4Codec implements Codec {

    private final LZ4Factory factory = LZ4Factory.fastestInstance();
    private final boolean highCompression;

    public LZ4Codec() {
        this(false);
    }

    public LZ4Codec(boolean highCompression) {
        this.highCompression = highCompression;
    }

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        LZ4Compressor compressor = highCompression ? factory.highCompressor() : factory.fastCompressor();
        compressor.compress(src, dst);
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        LZ4SafeDecompressor decompressor = factory.safeDecompressor();
        decompressor.decompress(src, dst);
    }
}
