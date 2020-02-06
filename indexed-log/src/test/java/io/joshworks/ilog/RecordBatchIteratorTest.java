//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.io.buffers.BufferPool;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.core.util.TestUtils;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.ilog.index.KeyComparator;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.nio.ByteBuffer;
//import java.util.NoSuchElementException;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertTrue;
//
//public class RecordBatchIteratorTest {
//
//    private IndexedSegment segment;
//    private BufferPool pool = BufferPool.unpooled(4096, false);
//
//    @Before
//    public void setUp() {
//        segment = new IndexedSegment(TestUtils.testFile(LogUtil.segmentFileName(System.nanoTime(), 0)), Size.MB.ofInt(500), KeyComparator.LONG);
//    }
//
//    @After
//    public void tearDown() {
//        segment.delete();
//    }
//
//    @Test
//    public void next() {
//        int items = 10;
//        for (int i = 0; i < items; i++) {
//            segment.append(RecordUtils.create(i, String.valueOf(i)));
//        }
//
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(1024, false));
//
//        for (int i = 0; i < items; i++) {
//            assertTrue(it.hasNext());
//            ByteBuffer next = it.next();
//            System.out.println(RecordUtils.toString(next, Serializers.LONG, Serializers.STRING));
//            assertEquals(Long.valueOf(i), RecordUtils.readKey(next, Serializers.LONG));
//        }
//    }
//
//    @Test
//    public void next_read_multiple() {
//        int items = 10000;
//        for (int i = 0; i < items; i++) {
//            segment.append(RecordUtils.create(i, String.valueOf(i)));
//        }
//
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(48, false));
//
//        for (int i = 0; i < items; i++) {
//            assertTrue("Failed on " + i, it.hasNext());
//            ByteBuffer next = it.next();
//            assertEquals(Long.valueOf(i), RecordUtils.readKey(next, Serializers.LONG));
//        }
//    }
//
//    @Test
//    public void peek() {
//        int items = 10;
//        for (int i = 0; i < items; i++) {
//            segment.append(RecordUtils.create(i, String.valueOf(i)));
//        }
//
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(1024, false));
//
//        for (int i = 0; i < items; i++) {
//            assertTrue(it.hasNext());
//            ByteBuffer next = it.peek();
//            assertEquals(Long.valueOf(0), RecordUtils.readKey(next, Serializers.LONG));
//        }
//    }
//
//    @Test
//    public void hasNext() {
//        segment.append(RecordUtils.create(0, String.valueOf(0)));
//
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(1024, false));
//
//        assertTrue(it.hasNext());
//        ByteBuffer next = it.next();
//        for (int i = 0; i < 10; i++) {
//            assertFalse(it.hasNext());
//        }
//    }
//
//    @Test
//    public void peek_hasNext() {
//        int items = 10;
//        segment.append(RecordUtils.create(0, String.valueOf(0)));
//
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(1024, false));
//
//        assertTrue(it.hasNext());
//        ByteBuffer next = it.peek();
//        for (int i = 0; i < 10; i++) {
//            assertTrue(it.hasNext());
//        }
//    }
//
//    @Test(expected = NoSuchElementException.class)
//    public void calling_next_when_there_is_no_more_entries_should_throw_exception() {
//        RecordBatchIterator it = new RecordBatchIterator(segment, IndexedSegment.START, BufferPool.unpooled(1024, false));
//
//        assertFalse(it.hasNext());
//        it.next();
//    }
//}