package io.joshworks.ilog.record;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;

public class FileRecords extends AbstractRecords {

    private ReadableByteChannel src;
    private StripedBufferPool pool;
    private BufferRecords records;
    private ByteBuffer readBuffer;



    FileRecords(String poolName) {
        super(poolName);
    }

    void init(ReadableByteChannel src, int bufferSize, StripedBufferPool pool) {
        this.src = src;
        this.pool = pool;
    }

    private void readBatch() {
        if (readPos + bufferPos >= segment.writePosition()) {
            return;
        }

        int read = src.read(readBuffer);
        if (read <= 0) {
            return;
        }
        records.close();
        records = RecordPool.get("TODO - DEFINE");
        recordsIdx = 0;

        readPos += read;
        readBuffer.flip();
        records.init(readBuffer);
        readBuffer.compact();
    }

    @Override
    public Record2 poll() {
        return null;
    }

    @Override
    public Record2 peek() {
        return null;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) {
        return 0;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, int count) {
        return 0;
    }


    @Override
    public void close()  {
        src = null;
        pool.free();
    }

}
