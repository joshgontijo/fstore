package io.joshworks.ilog.lsm;

import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;

public class BufferBinarySearch {

    public static int binarySearch(ByteBuffer key, ByteBuffer data, int dataStart, int dataCount, int entrySize, KeyComparator comparator) {
        if (dataCount % entrySize != 0) {
            throw new IllegalArgumentException("Read buffer must be multiple of " + entrySize);
        }
        int entries = dataCount / entrySize;

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = dataStart + (mid * entrySize);
            if (readPos < dataStart || readPos > dataStart + dataCount) {
                throw new IndexOutOfBoundsException("Index out of bounds: " + readPos);
            }
            int cmp = comparator.compare(data, readPos, key, key.position());
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }

    public static int binarySearch(ByteBuffer key, ByteBuffer data, int dataStart, int dataCount, KeyComparator comparator) {
        return binarySearch(key, data, dataStart, dataCount, comparator.keySize(), comparator);
    }

}