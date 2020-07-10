package io.joshworks.ilog.index;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class ByteBufferBinarySearch {

    private ByteBufferBinarySearch() {
    }

    public static int binarySearch(ByteBuffer key, ByteBuffer data, int dataStart, int dataCount, int entrySize, int keySize) {
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
            int cmp = Buffers.compare(data, readPos, key, key.position(), keySize);
//            int cmp = comparator.compare(data, readPos, key, key.position());
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
