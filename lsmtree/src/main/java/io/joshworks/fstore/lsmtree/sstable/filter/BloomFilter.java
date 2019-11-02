package io.joshworks.fstore.lsmtree.sstable.filter;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.MemStorage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.extra.DataFile;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Format
 * Length -> 4bytes
 * Number of bits (m) -> 4bytes
 * Number of hash (k) -> 4bytes
 * Data -> long[]
 */
public class BloomFilter {

    private static final int HEADER_SIZE = Integer.BYTES * 3;

    BitSet hashes;
    private final long size;
    private BloomFilterHasher hash;
    private final int m; // The number of bits in the filter
    private int k; // Number of hash functions

    /**
     * @param n     The expected number of elements in the filter
     * @param p     The acceptable false positive rate
     */
    private BloomFilter(long n, double p) {
        this.m = getNumberOfBits(p, n);
        this.k = getOptimalNumberOfHashesByBits(n, this.m);
        this.hash = BloomFilterHasher.murmur64();
        this.hashes = new BitSet(this.m);
        this.size = m / Byte.SIZE;
    }

    /**
     * Used to load from file only
     *
     * @param hashes The table containing the data
     * @param m      The number of bits in the 'hashes'
     * @param k      The number of hash functions
     */
    private BloomFilter(BitSet hashes, int m, int k, long size) {
        this.hashes = hashes;
        this.size = size;
        this.hash = BloomFilterHasher.murmur64();
        this.m = m;
        this.k = k;
    }

    /**
     * Add an element to the container
     */
    public void add(ByteBuffer key) {
        for (int h : hash.hash(hashes.size(), k, key)) {
            hashes.set(h);
        }
    }

    /**
     * Returns true if the element is in the container.
     * Returns false with a probability ≈ 1-e^(-ln(2)² * m/n)
     * if the element is not in the container.
     **/
    public boolean contains(ByteBuffer key) {
        for (int h : hash.hash(hashes.size(), k, key))
            if (!hashes.get(h))
                return false;
        return true;
    }

    /**
     * Removes all of the elements from this filter.
     **/
    public void clear() {
        hashes.clear();
    }

    /**
     * Generate a unique hash representing the filter
     **/
    @Override
    public int hashCode() {
        return hashes.hashCode() ^ k;
    }

    /**
     * Merge another bloom filter into the current one.
     * After this operation, the current bloom filter contains all elements in
     * other.
     **/
    public void merge(BloomFilter other) {
        if (other.k != this.k || other.hashes.size() != this.hashes.size()) {
            throw new IllegalArgumentException("Incompatible bloom filters");
        }
        this.hashes.or(other.hashes);
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
    private static int getNumberOfBits(double p, long n) {
        return (int) (Math.abs(n * Math.log(p)) / (Math.pow(Math.log(2), 2)));
    }


    public void write(DataFile<Block> dataFile, Codec codec, BufferPool bufferPool) {
        long[] items = hashes.toLongArray();
        int dataLength = items.length * Long.BYTES;
        long totalSize = dataLength + HEADER_SIZE;

        if (totalSize > MemStorage.MAX_BUFFER_SIZE) {
            throw new IllegalStateException("Bloom filter too big");
        }

        int blockSize = Math.min(bufferPool.bufferSize() - RecordHeader.HEADER_OVERHEAD, Size.MB.ofInt(1));
        BlockFactory blockFactory = dataBlockFactory();
        BlockSerializer blockSerializer = new BlockSerializer(codec, blockFactory);
        Block block = blockFactory.create(blockSize);

        writeHeader(dataFile, bufferPool, items.length, dataLength);

        for (long item : items) {
            if (!block.add(item, Serializers.LONG, bufferPool)) {
                dataFile.add(block);
                block.clear();
                block.add(item, Serializers.LONG, bufferPool);
            }
        }
        if (!block.isEmpty()) {
            dataFile.add(block);
        }
    }

    private void writeHeader(DataFile<Block> dataFile, BufferPool bufferPool, int entries, int dataLength) {
        //Format
        //Data length -> 4bytes
        //entries -> 4bytes
        //Number of bits (m) -> 4bytes
        //Number of hashes (k) -> 4bytes
        //Data -> long[]
        BlockFactory blockFactory = headerBlockFactory();
        Block headerBlock = blockFactory.create(128);
        try (bufferPool) {
            headerBlock.add(dataLength, Serializers.INTEGER, bufferPool);
            headerBlock.add(entries, Serializers.INTEGER, bufferPool);
            headerBlock.add(this.m, Serializers.INTEGER, bufferPool);
            headerBlock.add(this.k, Serializers.INTEGER, bufferPool);
        }
        dataFile.add(headerBlock);
    }

    public static BloomFilter load(Iterator<Block> dataIt) {
        if (!dataIt.hasNext()) {
            throw new IllegalStateException("Could not find Bloom filter header block");
        }
        Block headerBlock = dataIt.next();

        int size = headerBlock.get(0).getInt();
        int entries = headerBlock.get(1).getInt();
        int m = headerBlock.get(2).getInt();
        int k = headerBlock.get(3).getInt();
        long[] longs = readEntries(dataIt, entries);

        BitSet bitSet = new BitSet(m);
        bitSet.or(BitSet.valueOf(longs));

        return new BloomFilter(bitSet, m, k, size);
    }

    private static long[] readEntries(Iterator<Block> dataIt, int numEntries) {
        long[] longs = new long[numEntries];
        int i = 0;
        while (dataIt.hasNext()) {
            Block block = dataIt.next();
            for (ByteBuffer entry : block) {
                longs[i++] = entry.getLong();
            }
        }
        return longs;
    }

    private static BlockFactory dataBlockFactory() {
        return Block.vlenBlock();
    }

    private static BlockFactory headerBlockFactory() {
        return Block.flenBlock(Integer.BYTES);
    }

    private static Block readHeader(DataFile<Block> data) {
        return data.get(0);
    }

    public long size() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter that = (BloomFilter) o;
        return Objects.equals(hashes, that.hashes);
    }
}