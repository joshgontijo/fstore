package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CachedSegmentTest {

    protected Log<String> segment;
    private File testFile;

    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        Storage storage = StorageProvider.raf(true).create(testFile, Size.MB.of(500));
        segment = new Segment<>(storage, Serializers.STRING, new DataStream(new SingleBufferThreadCachedPool(false)), "magic", Type.LOG_HEAD);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(segment);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void get_cached() {
        List<Long> positions = new ArrayList<>();
        int items = 5000000;
        for (int i = 0; i < items; i++) {
            long pos = segment.append(String.valueOf(i));
            positions.add(pos);
        }

        //not cached
        long start = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            Long pos = positions.get(i);
            String entry = segment.get(pos);
            assertEquals(String.valueOf(i), entry);
        }
        System.out.println("UNCACHED: " + (System.currentTimeMillis() - start));


        //cached
        start = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            Long pos = positions.get(i);
            String entry = segment.get(pos);
            assertEquals(String.valueOf(i), entry);
        }
        System.out.println("CACHED: " + (System.currentTimeMillis() - start));


    }
}
