package io.joshworks.es2.sstable;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.codec.std.DeflaterCodec;
import io.joshworks.fstore.core.codec.Codec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum BlockCodec {

    NONE((byte) 0, Codec.noCompression()),
    SNAPPY((byte) 1, new SnappyCodec()),
    LZ4_HIGH((byte) 2, new LZ4Codec(true)),
    LZ4_LOW((byte) 3, new LZ4Codec(false)),
    DEFLATE((byte) 4, new DeflaterCodec());

    static final Map<Byte, BlockCodec> codecs = new ConcurrentHashMap<>(Map.of(
            NONE.id, NONE,
            SNAPPY.id, SNAPPY,
            LZ4_LOW.id, LZ4_LOW,
            LZ4_HIGH.id, LZ4_HIGH,
            DEFLATE.id, DEFLATE));

    Codec codec;
    byte id;

    BlockCodec(byte id, Codec codec) {
        this.id = id;
        this.codec = codec;
    }

    public static Codec from(byte id) {
        BlockCodec codec = codecs.get(id);
        if (codec == null) throw new IllegalArgumentException("Invalid codec id " + id);
        return codec.codec;
    }


}
