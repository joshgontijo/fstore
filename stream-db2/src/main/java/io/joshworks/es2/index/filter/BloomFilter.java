package io.joshworks.es2.index.filter;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.hash.Murmur3;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Objects;

/**
 * Format
 * Length -> 4bytes
 * Number of bits (m) -> 8bytes
 * Number of hash (k) -> 4bytes
 * Data -> long[]
 */
public class BloomFilter {

    private static final int HEADER = (Integer.BYTES * 2) + Long.BYTES;

    private static final long MAX_VAL = (((long) Buffers.MAX_CAPACITY) * Byte.SIZE);

    private final BitSet bits;
    private final int k; // Number of hash functions
    private final long m; // The number of bits in the filter


    /**
     * @param n The expected number of elements in the filter
     * @param p The acceptable false positive rate
     */
    private BloomFilter(long n, double p) {
        this.m = getNumberOfBits(p, n);
        this.k = getOptimalNumberOfHashesByBits(n, m);
        this.bits = new BitSet((int) m);
    }

    private BloomFilter(long m, int k, BitSet bits) {
        this.m = m;
        this.k = k;
        this.bits = bits;
    }

    public static BloomFilter load(ByteBuffer data) {
        int len = data.getInt();
        long m = data.getLong();
        int k = data.getInt();

        var bits = new byte[len - HEADER];
        data.get(bits);

        return new BloomFilter(m, k, BitSet.valueOf(bits));
    }

    public long writeTo(SegmentChannel channel) {
        byte[] data = bits.toByteArray();
        var buffer = Buffers.allocate(HEADER + data.length, false);
        buffer.putInt(HEADER + data.length);
        buffer.putLong(this.m);
        buffer.putInt(this.k);
        buffer.put(data);
        return channel.append(buffer.flip());
    }

//    private static int bufferSize(long m) {
//        if (m > MAX_VAL) {
//            throw new IllegalArgumentException("Buffer size cannot be greater than " + Integer.MAX_VALUE + ": m is too big");
//        }
//        return (int) ((m % Byte.SIZE == 0) ? (m / Byte.SIZE) : (m / Byte.SIZE) + 1);
//    }

    public void add(ByteBuffer key, int offset, int len) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key, offset, len);
            bits.set((int) bitIdx);
        }
    }

    /**
     * Add an element to the container
     */
    public void add(ByteBuffer key) {
        add(key, key.position(), key.remaining());
    }

    private int hash(int ki, ByteBuffer val, int offset, int len) {
        long hash1 = Murmur3.hash64(val, offset, len);
        long hash2 = (hash1 >>> 32);

        long combinedHash = hash1 + (ki * hash2);
        // Flip all the bits if it's negative (guaranteed positive number)
        combinedHash = combinedHash < 0 ? ~combinedHash : combinedHash;
        return (int) (combinedHash % m);
    }


    public boolean contains(ByteBuffer key, int offset, int len) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key, offset, len);
            if (!bits.get((int) bitIdx))
                return false;
        }
        return true;
    }

    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(ByteBuffer key) {
        return contains(key, key.position(), key.remaining());
    }


    /**
     * Generate a unique hash representing the filter
     **/
    @Override
    public int hashCode() {
        return bits.hashCode() ^ k;
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
        return bits.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter that = (BloomFilter) o;
        return Objects.equals(bits, that.bits);
    }
}