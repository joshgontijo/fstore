//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.io.buffers.BufferPool;
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.core.util.TestUtils;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.ilog.index.RowKey;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.nio.ByteBuffer;
//
//import static org.junit.Assert.assertEquals;
//
//public class IndexedSegmentTest {
//
//    private IndexedSegment segment;
//    private BufferPool pool = BufferPool.unpooled(4096, false);
//
//    @Before
//    public void setUp() {
//        segment = new IndexedSegment(TestUtils.testFile(LogUtil.segmentFileName(0, 0)), Size.MB.ofInt(500), RowKey.LONG);
//    }
//
//    @After
//    public void tearDown() {
//        segment.delete();
//    }
//
//    @Test
//    public void write() {
//        int items = 10000;
//        for (long i = 0; i < items; i++) {
//            ByteBuffer record = create(i, "value-" + i);
//            segment.append(record);
//        }
//        readAll(items);
//    }
//
//    @Test
//    public void writeN() {
//        var records = Buffers.allocate(4096, false);
//        int items = 10000;
//        for (long i = 0; i < items; i++) {
//            ByteBuffer r = create(i, "value-" + i);
//            if (!addToBuffer(r, records)) {
//                writeRecords(records);
//                addToBuffer(r, records);
//            }
//        }
//        if (records.position() > 0) {
//            writeRecords(records);
//        }
//        readAll(items);
//    }
//
//    private void writeRecords(ByteBuffer records) {
//        records.flip();
//        segment.append(records);
//        records.clear();
//    }
//
//    private void readAll(int items) {
//        SegmentIterator recordIterator = new SegmentIterator(segment, IndexedSegment.START, pool);
//        long idx = 0;
//        while (recordIterator.hasNext()) {
//            ByteBuffer record = recordIterator.next();
//            int size = Record.sizeOf(record);
//            int keySize = Record.KEY_LEN.get(record);
//            long key = RecordUtils.readKey(record);
//            assertEquals(Long.BYTES, keySize);
//            assertEquals(idx, key);
//            idx++;
//        }
//
//        assertEquals(items, idx);
//    }
//
//    private static ByteBuffer create(long key, String value) {
//        return RecordUtils.create(key, Serializers.LONG, value, Serializers.STRING);
//    }
//
//    private static boolean addToBuffer(ByteBuffer record, ByteBuffer dst) {
//        if (dst.remaining() < Record.sizeOf(record)) {
//            return false;
//        }
//        Record.copyTo(record, dst);
//        return true;
//    }
//
//    private static ByteBuffer bufferOf(long value) {
//        return ByteBuffer.allocate(Long.BYTES).putLong(value).flip();
//    }
//
//}