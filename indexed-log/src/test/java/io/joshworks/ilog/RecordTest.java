package io.joshworks.ilog;

import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RecordTest {

    @Test
    public void keyLength() {
        Record record = create(1, 123);
        assertEquals(Integer.BYTES, record.keySize());

    }

    @Test
    public void dataLength() {
        Record record = create(1, 123);
        assertEquals(Integer.BYTES, record.valueSize());
    }

    @Test
    public void recordLength() {
        Record record = create(1, 123);
        assertEquals(Record.HEADER_BYTES + record.keySize() + record.valueSize(), record.size());
    }

    @Test
    public void readKeyBack() {
        Record record = create(1, 123);
        Integer key = Serializers.INTEGER.fromBytes(record.key());
        assertEquals(Integer.valueOf(1), key);
    }

    @Test
    public void readValueBack() {
        Record record = create(1, 123);
        assertEquals(123, readData(record));
    }

    @Test
    public void timestamp() {
        Record record = create(1, 123);
        long timestamp = record.timestamp();
        long min = TimeUnit.MILLISECONDS.toMinutes(timestamp);
        long now = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
        assertEquals(now, min); //minute tolerance
    }

    @Test
    public void checksum() {
        Record record = create(1, 123);
        int checksum = record.checksum();

        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES).putInt(readData(record)).flip();
        int expected = ByteBufferChecksum.crc32(bb);
        assertEquals(expected, checksum);
    }

    @Test
    public void fromBuffer() {
        Record record = create(1, 123);

        Record from = Record.from(record.buffer, true);

        assertEquals(record.keySize(), from.keySize());
        assertEquals(record.checksum(), from.checksum());
        assertEquals(record.valueSize(), from.valueSize());
        assertEquals(record.timestamp(), from.timestamp());
        assertEquals(readData(record), readData(from));
        assertEquals(record.key(), from.key());
    }

    @Test
    public void fromBufferBatch_buffer_copy() {
        testBufferFrom(true);
    }

    @Test
    public void fromBufferBatch_no_buffer_copy() {
        testBufferFrom(false);
    }

    private void testBufferFrom(boolean copy) {
        int items = 10;
        var batchBuffer = ByteBuffer.allocate(4096);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < items; i++) {
            Record record = create(i, i);
            batchBuffer.put(record.buffer);
            records.add(record);
        }
        batchBuffer.flip();

        for (int i = 0; i < items; i++) {
            Record record = records.get(i);
            Record read = Record.from(batchBuffer, copy);

            assertNotNull(read);
            assertEquals(Integer.valueOf(i), Serializers.INTEGER.fromBytes(read.key()));

            assertEquals(record.keySize(), read.keySize());
            assertEquals(record.checksum(), read.checksum());
            assertEquals(record.valueSize(), read.valueSize());
            assertEquals(record.timestamp(), read.timestamp());
            assertEquals(readData(record), readData(read));
            assertEquals(record.key(), read.key());
            assertEquals(Integer.valueOf(i), Serializers.INTEGER.fromBytes(read.key()));
        }
    }

    @Test
    public void fromBuffer_no_copy() {
        Record record = create(1, 123);

        Record from = Record.from(record.buffer, false);

        assertEquals(record.keySize(), from.keySize());
        assertEquals(record.checksum(), from.checksum());
        assertEquals(record.valueSize(), from.valueSize());
        assertEquals(record.timestamp(), from.timestamp());
        assertEquals(readData(record), readData(from));
        assertEquals(record.key(), from.key());

    }

    private Record create(int key, int val) {
        return Record.create(key, Serializers.INTEGER, val, Serializers.INTEGER, ByteBuffer.allocate(1024));
    }

    private static int readData(Record record) {
        var buffer = ByteBuffer.allocate(Integer.BYTES);
        record.readValue(buffer);
        return buffer.flip().getInt();
    }
}