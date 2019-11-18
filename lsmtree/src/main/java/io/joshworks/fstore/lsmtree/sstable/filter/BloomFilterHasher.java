package io.joshworks.fstore.lsmtree.sstable.filter;

import io.joshworks.fstore.core.hash.Murmur3;

import java.nio.ByteBuffer;

public interface BloomFilterHasher {

    int[] hash(int maximum, int k, ByteBuffer val);

    static BloomFilterHasher murmur64() {
        return new Murmur64();
    }

    class Murmur64 implements BloomFilterHasher {

        @Override
        public int[] hash(int m, int k, ByteBuffer val) {
            long bitSize = m;
            long hash64 = Murmur3.hash64(val);
            int hash1 = (int) hash64;
            int hash2 = (int) (hash64 >>> 32);

            int[] result = new int[k];
            for (int i = 1; i <= k; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                result[i - 1] = (int) (combinedHash % bitSize);
            }
            return result;
        }
    }


}
