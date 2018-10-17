//package io.joshworks.fstore.log;
//
//import io.joshworks.fstore.core.io.IOUtils;
//import io.joshworks.fstore.core.io.Mode;
//import io.joshworks.fstore.core.io.RafStorage;
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.log.record.DataStream;
//import io.joshworks.fstore.log.segment.Log;
//import io.joshworks.fstore.log.segment.Segment;
//import io.joshworks.fstore.log.segment.header.Type;
//import io.joshworks.fstore.log.cache.CacheManager;
//import io.joshworks.fstore.log.segment.cache.CachedSegment;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.fstore.testutils.FileUtils;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//
//public class CachedSegmentTest {
//
//    private static final int maxAge = -1;
//    private static final long maxSize = Size.MEGABYTE.toBytes(200);
//
//    private CacheManager cacheManager;
//    protected Log<String> segment;
//    private File testFile;
//
//
//    @Before
//    public void setUp() {
//        cacheManager = new CacheManager(maxSize, maxAge);
//        testFile = FileUtils.testFile();
//        segment = open(testFile);
//    }
//
//    @After
//    public void cleanup() {
//        IOUtils.closeQuietly(segment);
//        FileUtils.tryDelete(testFile);
//    }
//
//    Log<String> open(File file) {
//        Segment<String> delegate = new Segment<>(new RafStorage(file, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE), Serializers.STRING, new DataStream(), "magic", Type.LOG_HEAD);
//        return new CachedSegment<>(delegate, Serializers.STRING, cacheManager);
//    }
//
//    @Test
//    public void get_cached() {
//        List<Long> positions = new ArrayList<>();
//        int items = 3000000;
//        for (int i = 0; i < items; i++) {
//            long pos = segment.append(String.valueOf(i));
//            positions.add(pos);
//        }
//
//        //not cached
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < items; i++) {
//            Long pos = positions.get(i);
//            String entry = segment.get(pos);
//            assertEquals(String.valueOf(i), entry);
//        }
//        System.out.println("UNCACHED: " + (System.currentTimeMillis() - start));
//
//
//        //cached
//        start = System.currentTimeMillis();
//        for (int i = 0; i < items; i++) {
//            Long pos = positions.get(i);
//            String entry = segment.get(pos);
//            assertEquals(String.valueOf(i), entry);
//        }
//        System.out.println("CACHED: " + (System.currentTimeMillis() - start));
//
//
//    }
//}
