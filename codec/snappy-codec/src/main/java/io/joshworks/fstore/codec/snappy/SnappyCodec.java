package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyCodec implements Codec {

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        try {
            if (src.isDirect() && dst.isDirect()) {
                int compressedLen = Snappy.compress(src, dst);
                dst.position(dst.position() + compressedLen);
                src.position(src.position() + src.remaining());
                return;
            }
            if (src.isDirect()) {
                byte[] srcBytes = copy(src);
                int compressedLen = Snappy.compress(srcBytes, 0, srcBytes.length, dst.array(), dst.position());
                dst.position(dst.position() + compressedLen);
                return;
            }
            if (dst.isDirect()) {
                int maxCompressedLength = Snappy.maxCompressedLength(src.remaining());
                byte[] dstBytes = new byte[maxCompressedLength];
                int compressedBytes = Snappy.compress(src.array(), src.position(), src.remaining(), dstBytes, 0);
                dst.put(dstBytes, 0, compressedBytes);
                src.position(src.position() + src.remaining());
                return;
            }

            int compressedLen = Snappy.compress(src.array(), src.position(), src.remaining(), dst.array(), dst.position());
            dst.position(dst.position() + compressedLen);
            src.position(src.position() + src.remaining());
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        try {
            if (src.isDirect() && dst.isDirect()) {
                if (!Snappy.isValidCompressedBuffer(src)) {
                    throw new RuntimeException("Not a valid compressed data");
                }
                int uncompressed = Snappy.uncompress(src, dst);
                dst.position(dst.position() + uncompressed);
                src.position(src.position() + src.remaining());
                return;
            }
            if (src.isDirect()) {
                byte[] srcBytes = copy(src);
                int uncompressed = Snappy.uncompress(srcBytes, 0, srcBytes.length, dst.array(), dst.position());
                dst.position(dst.position() + uncompressed);
                return;
            }
            if (dst.isDirect()) {
                int srcRemaining = src.remaining();
                int uncompressedLen = Snappy.uncompressedLength(src.array(), src.position(), srcRemaining);
                byte[] dstBytes = new byte[uncompressedLen];
                int actualUncompressedSize = Snappy.uncompress(src.array(), src.position(), srcRemaining, dstBytes, 0);
                dst.put(dstBytes, 0, actualUncompressedSize);
                src.position(src.position() + srcRemaining);
                return;
            }
            int uncompressed = Snappy.uncompress(src.array(), src.position(), src.remaining(), dst.array(), dst.position());
            dst.position(dst.position() + uncompressed);
            src.position(src.position() + src.remaining());
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private byte[] copy(ByteBuffer bb) {
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        return data;
    }

}


