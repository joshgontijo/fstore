//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.io.buffers.BufferPool;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.core.util.TestUtils;
//import io.joshworks.ilog.index.KeyComparator;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//public class LogTest {
//
//    private static final int MAX_ENTRY_SIZE = 1024;
//    private static final int INDEX_SIZE = Size.MB.ofInt(5);
//
//    private Log log;
//    private BufferPool pool;
//
//    @Before
//    public void setUp() throws Exception {
//        var root = TestUtils.testFolder();
//        pool = BufferPool.unpooled(MAX_ENTRY_SIZE, false);
//        log = new Log<>(root, MAX_ENTRY_SIZE, INDEX_SIZE, 2, 1, FlushMode.ON_ROLL, pool, (f, is) -> new IndexedSegment(f, is, KeyComparator.LONG));
//    }
//
//    @After
//    public void tearDown() {
//        log.delete();
//    }
//
//    @Test
//    public void append() {
//        for (int i = 0; i < 1000000000; i++) {
//            log.append(RecordUtils.create(i, String.valueOf(i)));
//        }
//    }
//}