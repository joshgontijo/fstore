package io.joshworks.fstore.core.hash;

import java.nio.ByteBuffer;

public interface Hash {

    long hash64(byte[] data);
    long hash64(ByteBuffer data);
    long hash64(ByteBuffer data, int offset, int count);

    int hash32(byte[] data, int seed);
    int hash32(ByteBuffer data);
    int hash32(ByteBuffer data, int seed);
    int hash32(ByteBuffer data, int offset, int count);
    int hash32(byte[] data);



}
