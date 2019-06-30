package io.joshworks.fstore.log.record;

import java.io.Closeable;
import java.nio.ByteBuffer;

class Record implements Closeable {

    private final ByteBuffer[] buffers = allocateBuffers();

    private static ByteBuffer[] allocateBuffers() {
        return new ByteBuffer[]{ByteBuffer.allocate(RecordHeader.MAIN_HEADER), null, ByteBuffer.allocate(RecordHeader.SECONDARY_HEADER)};
    }

    ByteBuffer[] create(ByteBuffer data) {
        fillBuffers(buffers, data);
        return buffers;
    }


    public static ByteBuffer[] create(ByteBuffer[] items) {
        ByteBuffer[] records = new ByteBuffer[items.length];
        for (int i = 0; i < items.length; i++) {
            var buffers = allocateBuffers();
            var data = items[i];
            fillBuffers(buffers, data);
            System.arraycopy(buffers, 0, records, i * buffers.length, buffers.length);
        }
        return records;
    }

    private static void fillBuffers(ByteBuffer[] buffers, ByteBuffer data) {
        int entrySize = data.remaining();

        buffers[0].putInt(entrySize);
        buffers[0].putInt(ByteBufferChecksum.crc32(data));
        buffers[0].flip();

        buffers[1] = data;

        buffers[2].putInt(entrySize);
        buffers[2].flip();
    }

    long size() {
        return buffers[0].remaining() + buffers[1].remaining() + buffers[2].remaining();
    }

    @Override
    public void close()  {
        buffers[0].clear();
        buffers[1] = null;
        buffers[2].clear();
    }
}
