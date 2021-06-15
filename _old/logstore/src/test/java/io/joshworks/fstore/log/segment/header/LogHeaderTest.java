package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.WriteMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public abstract class LogHeaderTest {

    private static final int STORAGE_SIZE = Size.MB.ofInt(1);
    private Storage storage;
    private File testFile;
    private DataStream stream;

    protected abstract StorageMode store();

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        testFile.deleteOnExit();
        storage = Storage.create(testFile, store(), STORAGE_SIZE);
        stream = new DataStream(new ThreadLocalBufferPool( STORAGE_SIZE, false), storage);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        TestUtils.deleteRecursively(testFile);
    }

    @Test
    public void header_restores_open_state() {
        LogHeader created = LogHeader.read(stream);
        created.writeNew(WriteMode.LOG_HEAD, 123, 456, true);

        LogHeader loaded = LogHeader.read(stream);
        assertEquals(created, loaded);
    }

    @Test
    public void header_restores_deleted_state() {
        LogHeader created = LogHeader.read(stream);
        created.writeDeleted();

        LogHeader loaded = LogHeader.read(stream);
        assertEquals(created, loaded);
    }

    @Test
    public void header_restores_rolled_state() {
        LogHeader created = LogHeader.read(stream);
        created.writeCompleted(1, 2, 3, 4, 5, 6, 7, 8);

        LogHeader loaded = LogHeader.read(stream);
        assertEquals(created, loaded);
    }

    @Test
    public void all_sections_are_restored_when_loaded() {
        LogHeader created = LogHeader.read(stream);
        created.writeNew(WriteMode.LOG_HEAD, 123, 456, true);
        created.writeDeleted();
        created.writeCompleted(1, 2, 3, 4, 5, 6, 7, 8);

        LogHeader loaded = LogHeader.read(stream);
        assertEquals(created, loaded);
    }

    public static class MmapLogHeaderTest extends LogHeaderTest {
        @Override
        protected StorageMode store() {
            return StorageMode.MMAP;
        }
    }

    public static class RafCachedLogHeaderTest extends LogHeaderTest {
        @Override
        protected StorageMode store() {
            return StorageMode.RAF_CACHED;
        }
    }

    public static class RafLogHeaderTest extends LogHeaderTest {
        @Override
        protected StorageMode store() {
            return StorageMode.RAF;
        }
    }

    public static class HeapLogHeaderTest extends LogHeaderTest {
        @Override
        protected StorageMode store() {
            return StorageMode.HEAP;
        }
    }

    public static class OffHeapLogHeaderTest extends LogHeaderTest {
        @Override
        protected StorageMode store() {
            return StorageMode.OFF_HEAP;
        }
    }

}