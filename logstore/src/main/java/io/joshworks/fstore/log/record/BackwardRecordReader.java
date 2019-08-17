package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;

final class BackwardRecordReader extends BaseReader implements Reader {

    BackwardRecordReader(BufferPool bufferPool, double checksumProb, int bufferSize) {
        super(bufferPool, checksumProb, bufferSize);
    }

    @Override
    public <T> RecordEntry<T> read(Storage storage, final long position, Serializer<T> serializer) {
        try (bufferPool) {
            ByteBuffer buffer = bufferPool.allocate();
            buffer.limit(Math.min(pageReadSize, buffer.capacity()));
            int limit = buffer.limit();
            if (position - limit < Log.START) {
                int available = (int) (position - Log.START);
                if (available == 0) {
                    return RecordEntry.empty();
                }
                buffer.limit(available);
                limit = available;
            }

            storage.read(position - limit, buffer);
            buffer.flip();
            if (buffer.remaining() == 0) {
                return RecordEntry.empty();
            }

            int recordDataEnd = buffer.limit() - RecordHeader.SECONDARY_HEADER;
            int length = buffer.getInt(recordDataEnd);
            if (length == 0) {
                return RecordEntry.empty();
            }

            int recordSize = length + RecordHeader.HEADER_OVERHEAD;

            if (recordSize > buffer.limit()) {
                bufferPool.free();
                buffer = bufferPool.allocate();
                buffer.limit(Math.min(recordSize, buffer.capacity()));

                buffer.limit(recordSize - RecordHeader.SECONDARY_HEADER); //limit to the entry size, excluding the secondary header
                long readStart = position - recordSize;
                storage.read(readStart, buffer);
                buffer.flip();

                int foundLength = buffer.getInt();
                return readRecord(position, serializer, buffer, foundLength);
            }

            buffer.limit(recordDataEnd);
            buffer.position(recordDataEnd - length - RecordHeader.CHECKSUM_SIZE);
            return readRecord(position, serializer, buffer, length);

        } finally {
            bufferPool.free();
        }

    }

    private <T> RecordEntry<T> readRecord(long position, Serializer<T> serializer, ByteBuffer buffer, int foundLength) {
        int checksum = buffer.getInt();
        checksum(checksum, buffer, position);
        T entry = serializer.fromBytes(buffer);
        return new RecordEntry<>(foundLength, entry, position);
    }
}
