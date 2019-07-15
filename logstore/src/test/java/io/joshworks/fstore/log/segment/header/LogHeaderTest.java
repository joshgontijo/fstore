package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public abstract class LogHeaderTest {

    private static final int STORAGE_SIZE = Size.MB.ofInt(1);
    private Storage storage;
    private File testFile;

    protected abstract StorageMode store();

    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        testFile.deleteOnExit();
        storage = Storage.create(testFile, store(), STORAGE_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void header_restores_open_state() {
        LogHeader created = LogHeader.read(storage);
        created.writeNew(WriteMode.LOG_HEAD, 123, 456, true);

        LogHeader loaded = LogHeader.read(storage);
        assertEquals(created, loaded);
    }

    @Test
    public void header_restores_deleted_state() {
        LogHeader created = LogHeader.read(storage);
        created.writeDeleted();

        LogHeader loaded = LogHeader.read(storage);
        assertEquals(created, loaded);
    }

    @Test
    public void header_restores_rolled_state() {
        LogHeader created = LogHeader.read(storage);
        created.writeCompleted(1, 2, 3, 4, 5, 6);

        LogHeader loaded = LogHeader.read(storage);
        assertEquals(created, loaded);
    }

    @Test
    public void all_sections_are_restored_when_loaded() {
        LogHeader created = LogHeader.read(storage);
        created.writeNew(WriteMode.LOG_HEAD, 123, 456, true);
        created.writeDeleted();
        created.writeCompleted(1, 2, 3, 4, 5, 6);

        LogHeader loaded = LogHeader.read(storage);
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