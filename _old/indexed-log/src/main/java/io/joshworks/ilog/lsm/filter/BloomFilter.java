package io.joshworks.ilog.lsm.filter;

import io.joshworks.fstore.core.hash.Murmur3;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Format
 * Length -> 4bytes
 * Number of bits (m) -> 4bytes
 * Number of hash (k) -> 4bytes
 * Data -> long[]
 */
public class BloomFilter {

    private final BitArray hashes;
    private final int k; // Number of hash functions
    private final long m; // The number of bits in the filter

    /**
     * @param n The expected number of elements in the filter
     * @param p The acceptable false positive rate
     */
    public BloomFilter(long n, double p) {
        this.m = getNumberOfBits(p, n);
        this.k = getOptimalNumberOfHashesByBits(n, m);

        this.hashes = new BitArray(m, true);
//        this.hashes.putInt(0, k);
//        this.hashes.putLong(Integer.BYTES, m);
    }

    /**
     * Add an element to the container
     */
    public void add(ByteBuffer key) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key);
            hashes.set(bitIdx);
        }
    }

    public long hash(int ki, ByteBuffer val) {
        long hash1 = Murmur3.hash64(val);
        long hash2 = (hash1 >>> 32);

        long combinedHash = hash1 + (ki * hash2);
        // Flip all the bits if it's negative (guaranteed positive number)
        combinedHash = combinedHash < 0 ? ~combinedHash : combinedHash;
        return combinedHash % m;
    }


    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(ByteBuffer key) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key);
            if (!hashes.get(bitIdx))
                return false;
        }
        return true;
    }


    /**
     * Generate a unique hash representing the filter
     **/
    @Override
    public int hashCode() {
        return hashes.hashCode() ^ k;
    }

    /**
     * k = (m / n) ln 2 from wikipedia.
     *
     * @param n the number of elements expected.
     * @param m the number of bytes allowed.
     * @return the best number of hashes.
     */
    private int getOptimalNumberOfHashesByBits(long n, long m) {
        return (int) Math.ceil(Math.log(2) * ((double) m / n));
    }


    /**
     * Calculate the number of bits needed to produce the provided probability of false
     * positives with the given element position.
     *
     * @param p The probability of false positives.
     * @param n The estimated number of elements.
     * @return The number of bits.
     */
    private static long getNumberOfBits(double p, long n) {
        return (long) (Math.abs(n * Math.log(p)) / (Math.pow(Math.log(2), 2)));
    }

    public long size() {
        return hashes.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter that = (BloomFilter) o;
        return Objects.equals(hashes, that.hashes);
    }
}