package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;

final class ForwardRecordReader extends BaseReader implements Reader {

    ForwardRecordReader(BufferPool bufferPool, double checksumProb, int bufferSize) {
        super(bufferPool, checksumProb, bufferSize);
    }

    @Override
    public <T> RecordEntry<T> read(Storage storage, final long position, Serializer<T> serializer) {
        ByteBuffer buffer = bufferPool.allocate(pageReadSize);
        try {
            int read = storage.read(position, buffer);
            if (read == Storage.EOF) {
                return null;
            }
            buffer.flip();

            if (buffer.remaining() < RecordHeader.MAIN_HEADER) {
                return null;
            }

            int length = buffer.getInt();
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
            return new RecordEntry<>(length, data, position);
        } finally {
            bufferPool.free(buffer);
        }
    }
}
