package io.joshworks.fstore.tcp.codec;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.codec.std.ZLibCodec;
import io.joshworks.fstore.core.codec.Codec;

import java.util.Map;

public class CodecRegistry {
    private static final Map<Compression, Codec> codecs = Map.of(
            Compression.LZ4_FAST, new LZ4Codec(),
            Compression.LZ4_HIGH, new LZ4Codec(true),
            Compression.SNAPPY, new SnappyCodec(),
            Compression.DEFLATE, new ZLibCodec()
    );


    public static Codec lookup(Compression compression) {
        return codecs.get(compression);
    }
}
