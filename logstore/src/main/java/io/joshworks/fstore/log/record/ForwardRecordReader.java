package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;

final class ForwardRecordReader extends BaseReader implements Reader {

    public ForwardRecordReader(double checksumProb, int maxEntrySize, int bufferSize) {
        super(checksumProb, maxEntrySize, bufferSize);
    }

    @Override
    public <T> RecordEntry<T> read(Storage storage, BufferPool bufferPool, long position, Serializer<T> serializer) {
        ByteBuffer buffer = bufferPool.allocate(bufferSize);
        try {
            storage.read(position, buffer);
            buffer.flip();

            if (buffer.remaining() < RecordHeader.MAIN_HEADER) {
                return null;
            }

            int length = buffer.getInt();
            checkRecordLength(length, position);
            if (length == 0) {
                return null;
            }

            int recordSize = length + RecordHeader.HEADER_OVERHEAD;
            if (recordSize > buffer.limit()) {
                bufferPool.free(buffer);
                buffer = bufferPool.allocate(recordSize);
                storage.read(position, buffer);
                buffer.flip();
                buffer.getInt(); //skip length
            }

            int checksum = buffer.getInt();
            buffer.limit(buffer.position() + length);
            checksum(checksum, buffer, position);
            T data = serializer.fromBytes(buffer);
            return new RecordEntry<>(length, data);
        } finally {
            bufferPool.free(buffer);
        }
    }
}
