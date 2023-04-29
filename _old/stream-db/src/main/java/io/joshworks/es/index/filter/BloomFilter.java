package io.joshworks.es.index.filter;

import io.joshworks.fstore.core.hash.Murmur3;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Objects;

/**
 * Format
 * Length -> 4bytes
 * Number of bits (m) -> 4bytes
 * Number of hash (k) -> 4bytes
 * Data -> long[]
 */
public class BloomFilter implements Closeable {

    private static final long MAX_VAL = (((long) Buffers.MAX_CAPACITY) * Byte.SIZE);

    private final MappedFile mf;
    private final BitArray hashes;
    private final int k; // Number of hash functions
    private final long m; // The number of bits in the filter


    /**
     * @param n The expected number of elements in the filter
     * @param p The acceptable false positive rate
     */
    private BloomFilter(long n, double p, File file) {
        if (!Files.exists(file.toPath())) {
            this.m = getNumberOfBits(p, n);
            this.k = getOptimalNumberOfHashesByBits(n, m);
            int bufferSize = bufferSize(m);
            this.mf = MappedFile.create(file, bufferSize);
        } else {
            //TODO not storing m an k in the file, after creating values must be always the same
            this.m = getNumberOfBits(p, n);
            this.k = getOptimalNumberOfHashesByBits(n, m);
            this.mf = MappedFile.open(file);
        }

        this.hashes = new BitArray(Buffers.allocate(bufferSize(m), false));
//        this.hashes.putInt(0, k);
//        this.hashes.putLong(Integer.BYTES, m);
    }

    /**
     * @param items         expected number of items
     * @param falsePositive value from 0 to 1
     * @param file          The destination file
     */
    public static BloomFilter createOrOpen(long items, double falsePositive, File file) {
        return new BloomFilter(items, falsePositive, file);
    }

    private static int bufferSize(long m) {
        if (m > MAX_VAL) {
            throw new IllegalArgumentException("Buffer size cannot be greater than " + Integer.MAX_VALUE + ": m is too big");
        }

        int bsize = (int) ((m % Byte.SIZE == 0) ? (m / Byte.SIZE) : (m / Byte.SIZE) + 1);
        return bsize;
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

    public void add(ByteBuffer key, int offset, int len) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key, offset, len);
            hashes.set(bitIdx);
        }
    }

    /**
     * Add an element to the container
     */
    public void add(ByteBuffer key) {
        add(key, key.position(), key.remaining());
    }

    private long hash(int ki, ByteBuffer val, int offset, int len) {
        long hash1 = Murmur3.hash64(val, offset, len);
        long hash2 = (hash1 >>> 32);

        long combinedHash = hash1 + (ki * hash2);
        // Flip all the bits if it's negative (guaranteed positive number)
        combinedHash = combinedHash < 0 ? ~combinedHash : combinedHash;
        return combinedHash % m;
    }

    public boolean contains(ByteBuffer key, int offset, int len) {
        for (int i = 0; i < k; i++) {
            long bitIdx = hash(i, key, offset, len);
            if (!hashes.get(bitIdx))
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

    @Override
    public void close() {
        mf.flush();
        mf.close();
    }

    public void delete() {
        mf.delete();
    }

    public void flush() {
        mf.flush();
    }
}