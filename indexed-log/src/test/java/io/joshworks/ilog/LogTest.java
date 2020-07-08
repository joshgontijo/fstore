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
//public class LogTest {
//
//    private static final int MAX_ENTRY_SIZE = 1024;
//    private static final int INDEX_SIZE = Size.MB.ofInt(5);
//
//    private Log<IndexedSegment> log;
//
//    @Before
//    public void setUp() throws Exception {
//        var root = TestUtils.testFolder();
//        BufferPool pool = BufferPool.unpooled(MAX_ENTRY_SIZE, false);
//        log = new Log<>(root, INDEX_SIZE, 2, 1, FlushMode.ON_ROLL, RowKey.LONG, IndexedSegment::new);
//    }
//
//    @After
//    public void tearDown() {
//        log.delete();
//    }
//
//    @Test
//    public void append() {
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
//
//    }
//
//    private void readAll(int items) {
//        LogIterator recordIterator = log.iterator();
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
//    private void writeRecords(ByteBuffer records) {
//        records.flip();
//        log.append(records);
//        records.clear();
//    }
//
//}