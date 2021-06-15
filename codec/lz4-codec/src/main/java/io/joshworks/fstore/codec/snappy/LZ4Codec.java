package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.codec.Codec;
import net.jpountz.lz4.LZ4Factory;

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
        var compressor = highCompression ? factory.highCompressor() : factory.fastCompressor();
        compressor.compress(src, dst);
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        var decompressor = factory.fastDecompressor();
        decompressor.decompress(src, dst);
    }

    @Override
    public String toString() {
        return highCompression ? "LZ4_HIGH" : "LZ4";
    }
}
