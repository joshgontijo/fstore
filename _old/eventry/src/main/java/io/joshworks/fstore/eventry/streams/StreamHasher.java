package io.joshworks.fstore.eventry.streams;

import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;

import java.nio.charset.StandardCharsets;

public class StreamHasher {

    private static final Hash highHasher = new XXHash();
    private static final Hash lowHasher = new Murmur3Hash();

    private static final long MASK = (1L << Integer.SIZE) - 1;

    public static long hash(String stream) {
        byte[] data = stream.getBytes(StandardCharsets.UTF_8);
        long high = ((long) highHasher.hash32(data)) << Integer.SIZE;
        int low = lowHasher.hash32(data);

        return high | (MASK & low);
    }

}
