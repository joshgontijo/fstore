package io.joshworks.fstore.core.io;

import java.io.Closeable;
import java.io.Flushable;
import java.nio.ByteBuffer;

public interface Storage extends Flushable, Closeable {

    int EOF = -1;

    int write(ByteBuffer data);

    int read(long position, ByteBuffer data);

    long length();

    void writePosition(long position);

    long writePosition();

    void delete();

    String name();

    void truncate();

    default boolean hasEnoughSpace(int dataSize) {
        long position = writePosition();
        long size = length();
        return position + dataSize < size;
    }

    default void validateWriteAddress(long position) {
        if (position < 0 || position >= length()) {
            long size = length();
            throw new StorageException("Invalid position: " + position + ", valid range: 0 to " + (size - 1));
        }
    }

    default boolean hasAvailableData(long readPos) {
        long writePos = writePosition();
        return readPos < writePos;
    }


    static void ensureNonEmpty(ByteBuffer data) {
        if (data.remaining() == 0) {
            throw new StorageException("Cannot store empty record");
        }
    }

}
