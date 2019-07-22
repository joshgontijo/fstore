package io.joshworks.fstore.codec.test;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.util.Size;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class CodecTest {

    private Codec codec;

    public abstract Codec codec();

    @Before
    public void setUp() {
        this.codec = codec();
    }

    @Test
    public void compress_decompress_heap_heap_heap() {
        testCompress(false, false, false);
    }

    @Test
    public void compress_decompress_direct_direct_direct() {
        testCompress(true, true, true);
    }

    //-----------

    @Test
    public void compress_decompress_direct_heap_heap() {
        testCompress(true, false, false);
    }

    @Test
    public void compress_decompress_heap_direct_heap() {
        testCompress(false, true, false);
    }

    @Test
    public void compress_decompress_heap_heap_direct() {
        testCompress(false, false, true);
    }

    //-----------------

    @Test
    public void compress_decompress_direct_direct_heap() {
        testCompress(true, true, false);
    }

    @Test
    public void compress_decompress_heap_direct_direct() {
        testCompress(false, true, true);
    }

    @Test
    public void compress_decompress_direct_heap_direct() {
        testCompress(true, false, true);
    }

    private void testCompress(boolean srcDirect, boolean compressedDirect, boolean uncompressedDirect) {
        for (int seed = 1; seed <= 1000; seed++) {
            var src = randBytes(seed, srcDirect);
            int dataSize = src.remaining();
            var compressed = allocate(Size.MB.ofInt(1), compressedDirect);
            codec.compress(src, compressed);

            compressed.flip();
            assertTrue("Compressed bytes be update its position", compressed.remaining() > 0);
            assertEquals("Compression source must consume all its remaining bytes", 0, src.remaining());

            //bigger buffer size to avoid exact buffer issues
            var uncompressed = allocate(src.position(), uncompressedDirect);

            codec.decompress(compressed, uncompressed);

            assertEquals("Uncompressed bytes must be ready to be flipped", dataSize, uncompressed.position());
            assertEquals("Compressed source must consume its bytes", 0, compressed.remaining());
            assertEquals("Original and uncompressed must be the same", src.position(), uncompressed.position());

            src.flip();
            byte[] srcBytes = new byte[src.remaining()];
            src.get(srcBytes);

            uncompressed.flip();
            byte[] uncompressedBytes = new byte[uncompressed.remaining()];
            uncompressed.get(uncompressedBytes);

            assertArrayEquals(srcBytes, uncompressedBytes);
        }
    }

    private static ByteBuffer allocate(int size, boolean direct) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    private static ByteBuffer randBytes(int dataLen, boolean direct) {
        String data = IntStream.range(0, dataLen).boxed().map(s -> UUID.randomUUID().toString()).collect(Collectors.joining(""));
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        var bb = allocate(bytes.length + 100, direct); //+100 is to avoid corner cases with exact size buffers
        bb.put(bytes);
        bb.flip();
        return bb;
    }


    public static class SnappyTest extends CodecTest {

        @Override
        public Codec codec() {
            return new SnappyCodec();
        }
    }

    public static class LZ4FastTest extends CodecTest {

        @Override
        public Codec codec() {
            return new LZ4Codec();
        }
    }

    public static class LZ4HighTest extends CodecTest {

        @Override
        public Codec codec() {
            return new LZ4Codec(true);
        }
    }

    public static class NoCodec extends CodecTest {

        @Override
        public Codec codec() {
            return Codec.noCompression();
        }
    }

}