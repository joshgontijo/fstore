package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSTableTest {

    private SSTable<Integer, String> sstable;
    private File testFile;


    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        sstable = open(testFile);
    }

    private SSTable<Integer, String> open(File file) {
        return new SSTable<>(file,
                StorageMode.MMAP,
                Size.MB.of(10),
                Serializers.INTEGER,
                Serializers.VSTRING,
                new BufferPool(),
                WriteMode.LOG_HEAD,
                VLenBlock.factory(),
                Codec.noCompression(),
                10000,
                0.01,
                Memory.PAGE_SIZE,
                50,
                120000,
                1,
                Memory.PAGE_SIZE);
    }

    @After
    public void tearDown() {
        sstable.close();
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void name() {
        int items = 1000000;
        for (int i = 0; i < items; i+=2) {
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1);

        for (int i = 1; i < items; i+=2) {
            Entry<Integer, String> floor = sstable.floor(i);
            assertNotNull(floor);
            assertEquals(Integer.valueOf(i - 1), floor.key);
        }

    }
}