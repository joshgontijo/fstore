package io.joshworks.es2.log;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Consumer;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TLogTest {

    private File folder;

    @Before
    public void init() {
        folder = TestUtils.testFolder();
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(folder);
    }

    @Test
    public void no_items_are_restored_for_new_log() {
        var restorer = new IntRestorer();
        TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(0, restorer.size());
    }

    @Test
    public void append_restore() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));
        log.append(of(2));

        log.close();
        TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(2, restorer.size());
        assertTrue(restorer.contains(1));
        assertTrue(restorer.contains(2));
    }

    @Test
    public void append_restore_multiple_segments() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));
        log.appendFlushEvent();
        log.append(of(2));

        log.roll();
        log.append(of(3));
        log.append(of(4));

        log.close();

        TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(3, restorer.size());
        assertTrue(restorer.contains(2));
        assertTrue(restorer.contains(3));
        assertTrue(restorer.contains(4));
    }

    @Test
    public void append_restore_last_entry() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));
        log.append(of(2));
        log.appendFlushEvent();

        log.roll();
        log.close();

        TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(0, restorer.size());
    }

    @Test
    public void append_restore_last_entry_with_entries_in_the_next_one() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));
        log.append(of(2));
        log.appendFlushEvent();

        log.roll();
        log.append(of(3));

        log.close();

        TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(1, restorer.size());
        assertTrue(restorer.contains(3));
    }

    @Test
    public void initial_sequence() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(-1, log.sequence());
    }

    @Test
    public void sequence_is_incremented() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));

        assertEquals(0, log.sequence());
    }

    @Test
    public void sequence_is_reloaded() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));

        log.close();

        log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(0, log.sequence());
    }

    @Test
    public void sequence_with_empty_head_is_loaded_correctly() {
        var restorer = new IntRestorer();
        var log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        log.append(of(1));
        log.roll();
        log.close();

        log = TLog.open(folder.toPath(), 4096, newSingleThreadExecutor(), restorer);
        assertEquals(0, log.sequence());
    }


    private static ByteBuffer of(int val) {
        return Buffers.allocate(Integer.BYTES, false).putInt(val).flip();
    }

    private static class IntRestorer extends ArrayList<Integer> implements Consumer<ByteBuffer> {

        @Override
        public void accept(ByteBuffer buffer) {
            add(buffer.getInt());
        }
    }

}