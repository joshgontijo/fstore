package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

final class BulkForwardRecordReader extends BaseReader implements BulkReader {


    public BulkForwardRecordReader(double checksumProb, int maxEntrySize, int bufferSize) {
        super(checksumProb, maxEntrySize, bufferSize);
    }

    @Override
    public <T> List<RecordEntry<T>> read(Storage storage, BufferPool bufferPool, long position, Serializer<T> serializer) {
        ByteBuffer buffer = bufferPool.allocate(bufferSize);

        final List<RecordEntry<T>> entries = new ArrayList<>();

        try {
            storage.read(position, buffer);
            buffer.flip();

            if (buffer.remaining() < RecordHeader.MAIN_HEADER) {
                return entries;
            }

            int length = buffer.getInt();
            checkRecordLength(length, position);
            if (length == 0) {
                return entries;
            }

            int recordSize = length + RecordHeader.HEADER_OVERHEAD;
            if (recordSize > buffer.limit()) {
                bufferPool.free(buffer);
                buffer = bufferPool.allocate(recordSize);
                storage.read(position, buffer);
                buffer.flip();
                buffer.getInt(); //skip length
            }

            buffer.position(buffer.position() - Integer.BYTES);
            int originalLimit = buffer.limit();
            while (buffer.hasRemaining() && buffer.remaining() > RecordHeader.MAIN_HEADER) {
                int pos = buffer.position();
                int len = buffer.getInt();
                checkRecordLength(len, position);
                if (len == 0 || buffer.remaining() < len + RecordHeader.CHECKSUM_SIZE) {
                    return entries;
                }

                int checksum = buffer.getInt();
                buffer.limit(buffer.position() + len);
                checksum(checksum, buffer, position + pos);

                T entry = serializer.fromBytes(buffer);
                entries.add(new RecordEntry<>(len, entry));

                int newPos = pos + len + RecordHeader.HEADER_OVERHEAD;
                newPos = Math.min(originalLimit, newPos);
                buffer.limit(originalLimit);
                if (newPos > buffer.limit()) {
                    return entries;
                }
                buffer.position(newPos);
            }
            return entries;
        } finally {
            bufferPool.free(buffer);
        }
    }
}
