package io.joshworks.es2.directory;


import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class SegmentDirectoryTest {

    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(root);
    }

    @Test
    public void name() {
        var dir = new SegmentDirectory<>(root,
                SegmentChannel::open,
                "test",
                Executors.newSingleThreadExecutor(),
                Compaction.noOp());


        dir.append(SegmentChannel.create(dir.newHead()));
        dir.append(SegmentChannel.create(dir.newHead()));
        dir.append(SegmentChannel.create(dir.newHead()));

        long nextIdx = dir.nextIdx(0);
        assertEquals(3, nextIdx);


    }
}