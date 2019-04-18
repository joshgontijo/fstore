package io.joshworks.fstore.codec.snappy;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
//FIXME
public class Lz4CodecTest {

    private final Lz4Codec codec = new Lz4Codec();

    @Test
    public void compress_decompress_heap() {
        for (int seed = 1; seed <= 1000; seed++) {
            var data = randBytes(seed, false);
            int compressedSize = data.limit();
            ByteBuffer compressed = codec.compress(data);
            ByteBuffer decompress = codec.decompress(compressed);
            assertEquals(data.clear(), decompress);
        }
    }

    @Test
    public void compress_decompress_offHeap() {
        for (int seed = 1; seed <= 1000; seed++) {
            var data = randBytes(seed, true);
            int compressedSize = data.limit();
            ByteBuffer compressed = codec.compress(data);
            ByteBuffer decompress = codec.decompress(compressed);
            assertEquals(data.clear(), decompress);
        }
    }

    private static ByteBuffer randBytes(int seed, boolean offheap) {
        String data = IntStream.range(0, seed).boxed().map(s -> UUID.randomUUID().toString()).collect(Collectors.joining(""));
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return offheap ? ByteBuffer.allocateDirect(bytes.length).put(bytes).flip() : ByteBuffer.wrap(bytes);
    }

}