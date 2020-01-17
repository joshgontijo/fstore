package io.joshworks.fstore.tcp.codec;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.codec.std.DeflaterCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.tcp.TcpMessage2;

import java.util.Map;

public class CodecRegistry {
    private static final Map<TcpMessage2.Compression, Codec> codecs = Map.of(
            TcpMessage2.Compression.LZ4_FAST, new LZ4Codec(),
            TcpMessage2.Compression.LZ4_HIGH, new LZ4Codec(true),
            TcpMessage2.Compression.SNAPPY, new SnappyCodec(),
            TcpMessage2.Compression.DEFLATE, new DeflaterCodec()
    );


    public static Codec lookup(TcpMessage2.Compression compression) {
        return codecs.get(compression);
    }
}
