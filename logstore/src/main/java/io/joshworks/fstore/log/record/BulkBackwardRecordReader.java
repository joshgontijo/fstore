package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

final class BulkBackwardRecordReader extends BaseReader implements BulkReader {


    BulkBackwardRecordReader(BufferPool bufferPool, double checksumProb, int bufferSize) {
        super(bufferPool, checksumProb, bufferSize);
    }

    @Override
    public <T> List<RecordEntry<T>> read(Storage storage, long position, Serializer<T> serializer) {
        ByteBuffer buffer = bufferPool.allocate(pageReadSize);

        List<RecordEntry<T>> entries = new ArrayList<>();

        try {
            int limit = buffer.limit();
            if (position - limit < Log.START) {
                int available = (int) (position - Log.START);
                if (available == 0) {
                    return entries;
                }
                buffer.limit(available);
                limit = available;
            }

            storage.read(position - limit, buffer);
            buffer.flip();
            if (buffer.remaining() == 0) {
                return entries;
            }

            int recordDataEnd = buffer.limit() - RecordHeader.SECONDARY_HEADER;
            int length = buffer.getInt(recordDataEnd);
            if (length == 0) {
                return entries;
            }

            int recordSize = length + RecordHeader.HEADER_OVERHEAD;

            if (recordSize > buffer.limit()) {
                bufferPool.free(buffer);
                buffer = bufferPool.allocate(recordSize);

                buffer.limit(recordSize); //limit to the entry size, excluding the secondary header
                long readStart = position - recordSize;
                storage.read(readStart, buffer);
                buffer.flip();
            }

            int originalLimit = buffer.limit();
            while (true) {
                buffer.limit(originalLimit);
                buffer.position(originalLimit);

                int len = buffer.getInt(originalLimit - RecordHeader.SECONDARY_HEADER);
                int recordStart = originalLimit - len - RecordHeader.HEADER_OVERHEAD;
                if (recordStart < 0) {
                    return entries;
                }
                buffer.position(originalLimit - len - RecordHeader.HEADER_OVERHEAD);

                int pos = buffer.position();
                if (len == 0 || buffer.remaining() < len + RecordHeader.CHECKSUM_SIZE) {
                    return entries;
                }

                buffer.getInt();//skip length
                int checksum = buffer.getInt();
                buffer.limit(buffer.position() + len);
                checksum(checksum, buffer, position + pos);
                T entry = serializer.fromBytes(buffer);
                entries.add(new RecordEntry<>(len, entry));

                originalLimit = pos;

                if (originalLimit - RecordHeader.SECONDARY_HEADER <= 0) {
                    return entries;
                }
            }
        } finally {
            bufferPool.free(buffer);
        }


    }
}
