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
    public ByteBuffer compress(ByteBuffer data) {
        int uncompressedLength = data.remaining();
        LZ4Compressor compressor = highCompression ? factory.highCompressor() : factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(uncompressedLength);
        ByteBuffer compressed = ByteBuffer.allocate(BLOCK_HEADER_SIZE + maxCompressedLength);
        compressed.position(BLOCK_HEADER_SIZE);
        compressor.compress(data, compressed);
        compressed.putInt(0, uncompressedLength);
        return compressed.flip();
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        int uncompressedSize = compressed.getInt();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        ByteBuffer decompressed = ByteBuffer.allocate(uncompressedSize);
        decompressor.decompress(compressed, decompressed);
        return decompressed.flip();
    }
}
