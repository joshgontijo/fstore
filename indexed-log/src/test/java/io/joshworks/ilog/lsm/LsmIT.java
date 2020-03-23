package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.ilog.LogIterator;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LsmIT {

    public static final KeyComparator COMPARATOR = KeyComparator.LONG;
    private Lsm lsm;
    private static final int MEM_TABLE_SIZE = 500000;

    @Before
    public void setUp() {
        lsm = Lsm.create(TestUtils.testFolder(), COMPARATOR)
                .memTable(MEM_TABLE_SIZE, Size.MB.ofInt(500), false)
                .codec(Codec.noCompression())
                .compactionThreshold(0)
                .compactionThreads(8)
                .open();

    }

    @After
    public void tearDown() {
        lsm.delete();
    }

    @Test
    public void iterator() {
        int items = 1000;
        for (int i = 0; i < items; i++) {
            lsm.append(LsmRecordUtils.add(i, String.valueOf(i)));
        }
        lsm.flush();

        LogIterator it = lsm.logIterator();
        while(it.hasNext()) {
            ByteBuffer next = it.next();
            System.out.println(Record.toString(next));
        }

    }

//    @Test
//    public void concurrent_write_read() throws InterruptedException {
//        int items = (int) (MEM_TABLE_SIZE * 20.5);
//        Thread writer = new Thread(() -> {
//            for (int i = 0; i < items; i++) {
//                lsm.append(LsmRecordUtils.add(i, String.valueOf(i)));
//                if(i % 100000 == 0) {
//                    System.out.println("WRITE " + i);
//                }
//            }
//            lsm.flush();
//        });
//
//        Thread reader = new Thread(() -> {
//            LogIterator it = lsm.logIterator();
//            var dst = Buffers.allocate(8096, false);
//            int entries = 0;
//            long lastKey = -1;
//
//            try {
//                File file = new File("result.txt");
//                file.createNewFile();
//                FileWriter writer1 = new FileWriter(file);
//
//
//            while (entries < items) {
//                if(it.read(dst) == 0) {
//                    Threads.sleep(5);
//                    continue;
//                }
//                dst.flip();
//                while (RecordBatch.hasNext(dst)) {
//                    long k = dst.getLong(dst.position() + Record.KEY.offset(dst));
//                    writer1.append(String.valueOf(it.segPos))
//                            .append(" -> ")
//                            .append(String.valueOf(dst.position()))
//                            .append(" -> ")
//                            .append(String.valueOf(k))
//                            .append(System.lineSeparator());
//                    writer1.flush();
//
//                    RecordBatch.advance(dst);
//                    assertEquals(lastKey + 1, k);
//                    lastKey = k;
//                    if(entries % 100000 == 0) {
//                        System.out.println("READ " + entries);
//                    }
//                    entries++;
//                }
//                dst.compact();
//            }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            assertEquals(items, entries);
//        });
//
//        writer.start();
//        reader.start();
//
//        writer.join();
//        reader.join();
//
//    }

    @Test
    public void append_flush() {
        int items = (int) (MEM_TABLE_SIZE * 1.5);
        ByteBuffer record = LsmRecordUtils.add(0, "value-123");
        ByteBuffer key2 = Buffers.allocate(8, false);
        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            key2.clear().putLong(i).flip();
            Record.KEY.set(record, key2);
            lsm.append(record);
            if (i % 1000000 == 0) {
                System.out.println("WRITTEN: " + i + " In " + (System.currentTimeMillis() - s));
                s = System.currentTimeMillis();
            }
        }

        var dst = Buffers.allocate(1024, false);
        var key = Buffers.allocate(8, false);
        for (int i = 0; i < items; i++) {
            key.clear().putLong(i).flip();
            int rsize = lsm.get(key, dst);
            dst.flip();
            assertTrue("Failed on " + i, rsize > 0);

            assertTrue("Failed on " + i, Record.isValid(dst));

            int compare = Record.compareToKey(dst, key, COMPARATOR);
            assertEquals("Keys are not equals", 0, compare);

            if (i % 1000000 == 0) {
                System.out.println("READ: " + i);
            }

        }
    }

    @Test
    public void readLog() {
        int items = 1000;
        for (int i = 0; i < items; i++) {
            lsm.append(LsmRecordUtils.add(i, "value-" + i));
        }

        for (int i = 0; i < items; i++) {
            var readBuffer = Buffers.allocate(4096, false);
            int read = lsm.readLog(readBuffer, i);
            assertTrue(read > 0);

            readBuffer.flip();
            assertTrue(Record.isValid(readBuffer));

            int kOffset = Record.KEY.offset(readBuffer);
            long id = readBuffer.getLong(kOffset);
            assertEquals(i, id);
        }
    }

    @Test
    public void delete() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.append(LsmRecordUtils.delete(0));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertTrue(Record.hasAttribute(dst, RecordFlags.DELETION_ATTR));
    }

    @Test
    public void update_no_flush_returns_last_entry() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.append(LsmRecordUtils.add(0, String.valueOf(1)));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertEquals("1", RecordUtils.readValue(dst));
    }

    @Test
    public void update_flush_returns_last_entry() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.flush();
        lsm.append(LsmRecordUtils.add(0, String.valueOf(1)));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertEquals("1", RecordUtils.readValue(dst));
    }

    private static ByteBuffer keyOf(long key) {
        return Buffers.allocate(Long.BYTES, false).putLong(key).flip();
    }

}