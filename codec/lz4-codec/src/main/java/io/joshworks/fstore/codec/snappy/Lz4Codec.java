package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.Codec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class Lz4Codec implements Codec {

    private final LZ4Factory factory = LZ4Factory.fastestInstance();
    private final boolean highCompression;

    public Lz4Codec() {
        this(false);
    }

    public Lz4Codec(boolean highCompression) {
        this.highCompression = highCompression;
    }

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        LZ4Compressor compressor = highCompression ? factory.highCompressor() : factory.fastCompressor();
        compressor.compress(src, dst);
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        decompressor.decompress(src, dst);
    }
}
