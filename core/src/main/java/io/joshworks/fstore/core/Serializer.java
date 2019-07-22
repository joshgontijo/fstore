package io.joshworks.fstore.core;

import java.nio.ByteBuffer;

public interface Serializer<T> {

    /**
     * Writes data to this {@link ByteBuffer}
     *
     * @param data The data to be put in the ByteBuffer
     * @param dst The destination buffer
     */
    void writeTo(T data, ByteBuffer dst);

    /**
     * @param buffer The buffer to read the data from, with the position at the beginning of the data to be read from
     * @return The new instance of the type
     */
    T fromBytes(ByteBuffer buffer);

}
