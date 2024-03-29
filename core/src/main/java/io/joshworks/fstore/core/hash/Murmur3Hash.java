package io.joshworks.fstore.core.hash;

import java.nio.ByteBuffer;

public class Murmur3Hash implements Hash {

    @Override
    public int hash32(ByteBuffer data) {
        return Murmur3.hash32(data);
    }

    @Override
    public long hash64(ByteBuffer data) {
        return Murmur3.hash64(data);
    }

    @Override
    public long hash64(ByteBuffer data, int offset, int count) {
        return Murmur3.hash64(data, offset, count);
    }

    @Override
    public int hash32(ByteBuffer data, int seed) {
        return Murmur3.hash32(data, seed);
    }

    @Override
    public int hash32(ByteBuffer data, int offset, int count) {
        return Murmur3.hash32(data, offset, count);
    }

    @Override
    public int hash32(byte[] data) {
        return Murmur3.hash32(data);
    }

    @Override
    public long hash64(byte[] data) {
        return Murmur3.hash64(data);
    }

    @Override
    public int hash32(byte[] data, int seed) {
        return Murmur3.hash32(data, seed);
    }
}
