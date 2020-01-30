package io.joshworks.ilog.lsm;

import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;

public class BufferBinarySearch {


    public static int binarySearch(ByteBuffer key, ByteBuffer data, int dataStart, int dataCount, KeyComparator comparator) {
        int keySize = key.remaining();
        if (comparator.keySize() != keySize) {
            throw new IllegalArgumentException("Invalid key size");
        }
        if (dataCount % keySize != 0) {
            throw new IllegalArgumentException("Read buffer must be multiple of " + keySize);
        }
        int entries = dataCount / keySize;

        int low = 0;
        int high = entries - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int readPos = dataStart + (mid * comparator.keySize());
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

}
