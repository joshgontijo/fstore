package io.joshworks.fstore.core.hash;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;

public class XXHash implements Hash {

    private final XXHashFactory factory = XXHashFactory.fastestInstance();
    private final XXHash32 hash32 = factory.hash32();
    private final XXHash64 hash64 = factory.hash64();
    private static final int SEED = 0x9747b28c;

    @Override
    public int hash32(ByteBuffer data) {
        return hash32(data, SEED);
    }

    @Override
    public long hash64(ByteBuffer data) {
        return hash64(data, data.position(), data.remaining());
    }

    @Override
    public long hash64(ByteBuffer data, int offset, int count) {
        return hash64.hash(data, offset, count, SEED);
    }

    @Override
    public int hash32(ByteBuffer data, int seed) {
        return hash32(data, data.position(), data.remaining());
    }

    @Override
    public int hash32(ByteBuffer data, int offset, int count) {
        return hash32.hash(data, offset, count, SEED);
    }

    @Override
    public int hash32(byte[] data) {
        return hash32(data, SEED);
    }

    @Override
    public long hash64(byte[] data) {
        return hash64.hash(data, 0, data.length, SEED);
    }

    @Override
    public int hash32(byte[] data, int seed) {
        return hash32.hash(data, 0, data.length, seed);
    }
}
