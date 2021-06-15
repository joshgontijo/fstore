package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.absoluteArrayPosition;
import static io.joshworks.fstore.core.io.buffers.Buffers.copyArray;

public class SnappyCodec implements Codec {

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        try {
            if (src.isDirect() && dst.isDirect()) {
                int limit = dst.limit();
                int compressedLen = Snappy.compress(src, dst);
                dst.position(dst.position() + compressedLen);
                dst.limit(limit); //this compression mode will update the limit, we need to revert
                src.position(src.position() + src.remaining());
                return;
            }
            if (src.isDirect()) {
                byte[] srcBytes = copyArray(src);
                int compressedLen = Snappy.compress(srcBytes, 0, srcBytes.length, dst.array(), absoluteArrayPosition(dst));
                dst.position(dst.position() + compressedLen);
                return;
            }
            if (dst.isDirect()) {
                int maxCompressedLength = Snappy.maxCompressedLength(src.remaining());
                byte[] dstBytes = new byte[maxCompressedLength];
                int compressedBytes = Snappy.compress(src.array(), absoluteArrayPosition(src), src.remaining(), dstBytes, 0);
                dst.put(dstBytes, 0, compressedBytes);
                src.position(src.position() + src.remaining());
                return;
            }

            int compressedLen = Snappy.compress(src.array(), absoluteArrayPosition(src), src.remaining(), dst.array(), absoluteArrayPosition(dst));
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
                byte[] srcBytes = copyArray(src);
                int uncompressed = Snappy.uncompress(srcBytes, 0, srcBytes.length, dst.array(), absoluteArrayPosition(dst));
                dst.position(dst.position() + uncompressed);
                return;
            }
            if (dst.isDirect()) {
                int srcRemaining = src.remaining();
                int uncompressedLen = Snappy.uncompressedLength(src.array(), absoluteArrayPosition(src), srcRemaining);
                byte[] dstBytes = new byte[uncompressedLen];
                int actualUncompressedSize = Snappy.uncompress(src.array(), absoluteArrayPosition(src), srcRemaining, dstBytes, 0);
                dst.put(dstBytes, 0, actualUncompressedSize);
                src.position(src.position() + srcRemaining);
                return;
            }
            int uncompressed = Snappy.uncompress(src.array(), absoluteArrayPosition(src), src.remaining(), dst.array(), absoluteArrayPosition(dst));
            dst.position(dst.position() + uncompressed);
            src.position(src.position() + src.remaining());
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public String toString() {
        return "SNAPPY";
    }
}


