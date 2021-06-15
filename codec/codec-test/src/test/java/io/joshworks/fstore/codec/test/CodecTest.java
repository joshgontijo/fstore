package io.joshworks.fstore.codec.test;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.codec.std.ZLibCodec;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.util.Size;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.Deflater;

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


    @Test
    public void compress_starts_from_the_current_buffer_position() {
        ByteBuffer bb = ByteBuffer.allocate(256);
        bb.putInt(123);
        bb.putInt(456);

        byte[] toBeCompressed = new byte[bb.remaining()];
        Arrays.fill(toBeCompressed, (byte) 1);
        bb.put(toBeCompressed);
        bb.position(Integer.BYTES * 2);

        ByteBuffer dst = ByteBuffer.allocate(bb.capacity());
        codec.compress(bb, dst);

        dst.flip();

        ByteBuffer decompressed = ByteBuffer.allocate(toBeCompressed.length);
        codec.decompress(dst, decompressed);

        assertArrayEquals(toBeCompressed, decompressed.array());
    }

    @Test
    public void compress_does_not_change_destination_buffer_limit() {
        ByteBuffer bb = ByteBuffer.allocate(256);
        bb.putInt(123);
        bb.putInt(456);

        ByteBuffer dst = ByteBuffer.allocate(bb.capacity());
        codec.compress(bb, dst);

        assertEquals("Compressor should not change the destination buffer limit", dst.limit(), dst.capacity());
    }

    @Test
    public void compress_does_not_change_destination_buffer_limit_DIRECT_DIRECT() {
        ByteBuffer bb = ByteBuffer.allocateDirect(256);
        bb.putInt(123);
        bb.putInt(456);

        ByteBuffer dst = ByteBuffer.allocateDirect(bb.capacity());
        codec.compress(bb, dst);

        assertEquals("Compressor should not change the destination buffer limit", dst.limit(), dst.capacity());
    }

    @Test
    public void compress_does_not_change_destination_buffer_limit_HEAP_DIRECT() {
        ByteBuffer bb = ByteBuffer.allocate(256);
        bb.putInt(123);
        bb.putInt(456);

        ByteBuffer dst = ByteBuffer.allocateDirect(bb.capacity());
        codec.compress(bb, dst);

        assertEquals("Compressor should not change the destination buffer limit", dst.limit(), dst.capacity());
    }

    @Test
    public void compress_does_not_change_destination_buffer_limit_DIRECT_HEAP() {
        ByteBuffer bb = ByteBuffer.allocateDirect(256);
        bb.putInt(123);
        bb.putInt(456);

        ByteBuffer dst = ByteBuffer.allocate(bb.capacity());
        codec.compress(bb, dst);

        assertEquals("Compressor should not change the destination buffer limit", dst.limit(), dst.capacity());
    }

    @Test
    public void compress_decompress_src_SLICED_BUFFER() {
        ByteBuffer srcSliced = randBytesSlicedBuffer(16, false);

        int extraLen = 200;
        ByteBuffer compressed = ByteBuffer.allocateDirect(srcSliced.capacity() + extraLen);
        codec.compress(srcSliced, compressed);
        compressed.flip();

        ByteBuffer decompressed = ByteBuffer.allocateDirect(srcSliced.capacity() + extraLen);

        codec.decompress(compressed, decompressed);

        assertByteBufferEquals(srcSliced.flip(), decompressed.flip());
    }

    @Test
    public void compress_decompress_dst_SLICED_BUFFER() {
        ByteBuffer srcSliced = randBytesSlicedBuffer(16, false);

        int extraLen = 200;
        ByteBuffer compressed = ByteBuffer.allocateDirect(srcSliced.capacity() + extraLen);
        codec.compress(srcSliced, compressed);
        compressed.flip();

        ByteBuffer dstSliced = ByteBuffer.allocateDirect(srcSliced.capacity() + extraLen)
                .position(1)
                .limit(srcSliced.capacity() + 10)
                .slice();

        codec.decompress(compressed, dstSliced);
        assertByteBufferEquals(srcSliced.flip(), dstSliced.flip());
    }

    private void testCompress(boolean srcDirect, boolean compressedDirect, boolean uncompressedDirect) {
        int items = 1000;
        double compression = 0;
        for (int seed = 1; seed <= items; seed++) {
            var src = randBytesSlicedBuffer(seed, srcDirect);
            int dataSize = src.remaining();
            var compressed = allocate(Size.MB.ofInt(1), compressedDirect);

            codec.compress(src, compressed);

            compressed.flip();

            int compressedSize = compressed.remaining();
            compression += (dataSize - compressedSize) / 100;


            assertTrue("Compressed bytes be update its position", compressed.remaining() > 0);
            assertEquals("Compression source must consume all its remaining bytes", 0, src.remaining());

            //bigger buffer size to avoid exact buffer issues
            var uncompressed = allocate(src.position(), uncompressedDirect);

            codec.decompress(compressed, uncompressed);
            assertByteBufferEquals(src.flip(), uncompressed.flip());
        }

        System.out.println(String.format(codec + ": AVERAGE COMPRESSION: %.2f", (compression / items)));
    }

    private static ByteBuffer allocate(int size, boolean direct) {
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    private static ByteBuffer randBytesSlicedBuffer(int dataLen, boolean direct) {
        String data = IntStream.range(0, dataLen).boxed().map(s -> UUID.randomUUID().toString()).collect(Collectors.joining(""));
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        var bb = allocate(bytes.length + 256, direct); //+256 is to avoid corner cases with exact size buffers

        bb.position(1);
        bb.put(bytes);
        bb.position(1);
        bb.limit(bb.position() + bytes.length);

        return bb.slice();
    }


    /**
     * Similar to {@link ByteBuffer#equals(Object)} but does not take the buffer type into consideration
     *
     * @param expected
     * @param actual
     */
    private static void assertByteBufferEquals(ByteBuffer expected, ByteBuffer actual) {
        expected.flip();
        byte[] expectedBytes = new byte[expected.remaining()];
        expected.get(expectedBytes);

        actual.flip();
        byte[] actualBytes = new byte[actual.remaining()];
        actual.get(actualBytes);

        assertArrayEquals("ByteBuffer contents are not equal", expectedBytes, actualBytes);
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

    public static class DeflaterTest extends CodecTest {

        @Override
        public Codec codec() {
            return new ZLibCodec();
        }
    }

    public static class DeflaterWrapTest extends CodecTest {

        @Override
        public Codec codec() {
            return new ZLibCodec(Deflater.DEFAULT_COMPRESSION, false);
        }
    }

    public static class DeflaterBestCompressionTest extends CodecTest {

        @Override
        public Codec codec() {
            return new ZLibCodec(Deflater.BEST_COMPRESSION, true);
        }
    }

}