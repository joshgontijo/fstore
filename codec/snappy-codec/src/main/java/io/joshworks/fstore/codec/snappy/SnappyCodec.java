package io.joshworks.fstore.codec.snappy;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyCodec implements Codec {

    @Override
    public ByteBuffer compress(ByteBuffer data) {
        try {
            byte[] bytes = getBytes(data);
            byte[] compressed = Snappy.compress(bytes);
            int dstLen = compressed.length + BLOCK_HEADER_SIZE;
            ByteBuffer dst = getBuffer(data.isDirect(), dstLen);
            dst.putInt(bytes.length);
            dst.put(compressed);
            return dst.flip();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private static ByteBuffer getBuffer(boolean direct, int dstLen) {
        return direct ? ByteBuffer.allocateDirect(dstLen) : ByteBuffer.allocate(dstLen);
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed) {
        try {
            compressed.getInt(); //ignore
            byte[] uncompressed = Snappy.uncompress(getBytes(compressed));
            var buffer = getBuffer(compressed.isDirect(), uncompressed.length);
            buffer.put(uncompressed);
            return buffer.flip();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private static byte[] getBytes(ByteBuffer data) {
        byte[] b = new byte[data.remaining()];
        data.mark();
        data.get(b);
        data.reset();
        return b;
    }
}


//TODO probably better alternative, need to get it to work
//    @Override
//    public ByteBuffer compress(ByteBuffer data) {
//        try {
//            if(data.isDirect()) {
//                return directCompress(data);
//            }
//            return heapCompress(data);
//        } catch (IOException e) {
//            throw RuntimeIOException.of(e);
//        }
//    }
//    @Override
//    public ByteBuffer decompress(int uncompressedSize, ByteBuffer compressed) {
//        try {
//            if(compressed.isDirect()) {
//                return directDecompress(uncompressedSize, compressed);
//            }
//            return heapDecompress(uncompressedSize, compressed);
//        } catch (IOException e) {
//            throw RuntimeIOException.of(e);
//        }
//    }
//    private static ByteBuffer directDecompress(int uncompressedSize, ByteBuffer src) throws IOException {
//        var dst = ByteBuffer.allocateDirect(uncompressedSize);
//        Snappy.uncompress(src, dst);
//        return dst;
//    }
//    private static ByteBuffer heapDecompress(int uncompressedSize, ByteBuffer src) throws IOException {
//        byte[] uncompressed = new byte[uncompressedSize];
//        Snappy.uncompress(src.array(), src.position(), src.remaining(), uncompressed, 0);
//        return ByteBuffer.wrap(uncompressed);
//    }
//    private static ByteBuffer directCompress(ByteBuffer src) throws IOException {
//        var dst = ByteBuffer.allocateDirect(src.remaining() + EXTRA);
//        Snappy.compress(src, dst);
//        return dst;
//    }
//    private static ByteBuffer heapCompress(ByteBuffer src) throws IOException {
//        byte[] dst = new byte[src.remaining() + EXTRA];
//        Snappy.compress(src.array(), src.position(), src.remaining(), dst, 0);
//        return ByteBuffer.wrap(dst);
//    }