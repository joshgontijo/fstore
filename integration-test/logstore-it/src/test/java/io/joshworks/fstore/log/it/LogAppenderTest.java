package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public abstract class LogAppenderTest {

    public static final long SEGMENT_SIZE = Size.MB.of(128);
    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(File testDirectory);

    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender(testDirectory);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void insert_get_1M() {
        int items = 1000000;
        String value = "A";

        appendN(value, items);
        appender.flush();

        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void reopening_after_shrinking_returns_all_data() {

        String data = "DATA";
        long position;
        String name;


        appender.append(data);
        position = appender.position();
        name = appender.currentSegment();
        appender.roll();

        appender.close();

        File f = new File(testDirectory, name);
        if (!Files.exists(f.toPath())) {
            fail("File " + f + " doesn't exist");
        }

        try (LogAppender<String> appender = appender(testDirectory)) {
            LogIterator<String> logIterator = appender.iterator(Direction.FORWARD);
            assertTrue(logIterator.hasNext());
            assertEquals(data, logIterator.next());
        }
    }

    @Test
    public void insert_reopen_scan_1M_2kb_entries() {
        int items = 1000000;
        String value = stringOfLength(2048);
        appendN(value, items);

        appender.close();

        appender = appender(testDirectory);

        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void insert_1M_with_1kb_entries() {
        String value = stringOfLength(1024);

        appendN(value, 1000000);
        appender.flush();

        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void insert_1M_with_2kb_entries() {
        String value = stringOfLength(2048);

        appendN(value, 1000000);
        appender.flush();

        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void insert_5M_with_512b_entries() {

        String value = stringOfLength(512);

        appendN(value, 5000000);
        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void insert_5M_with_512b_entries_backwards_scan() {

        String value = stringOfLength(512);

        appendN(value, 5000000);
        scanAllAssertingSameValue(value, Direction.BACKWARD);
    }

    @Test
    public void insert_5M_with_1kb_entries() {

        String value = stringOfLength(1024);

        appendN(value, 5000000);
        scanAllAssertingSameValue(value, Direction.FORWARD);
    }

    @Test
    public void insert_5M_with_1kb_entries_backwards_scan() {

        String value = stringOfLength(1024);

        appendN(value, 5000000);
        scanAllAssertingSameValue(value, Direction.BACKWARD);
    }

    @Test
    public void random_access_5M_with_1kb_entries() {

        String value = stringOfLength(1024).intern();
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 5000000; i++) {
            long pos = appender.append(value);
            positions.add(pos);
        }

        long start = System.currentTimeMillis();
        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long read = 0;
        long totalRead = 0;

        for (Long position : positions) {

            String val = appender.get(position);
            assertEquals(val, val);

            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + read) / 2;
                System.out.println("TOTAL READ: " + totalRead + " - LAST SECOND: " + read + " - AVG: " + avg);
                read = 0;
                lastUpdate = System.currentTimeMillis();
            }

            read++;
            totalRead++;
        }
        System.out.println("APPENDER_READ -  READ " + totalRead + " ENTRIES IN " + (System.currentTimeMillis() - start) + "ms");
    }

    @Test
    public void reopen() {

        appender.close();
        int iterations = 200;

        Long lastPosition = null;
        for (int i = 0; i < iterations; i++) {
            try (LogAppender<String> appender = appender(testDirectory)) {
                if (lastPosition != null) {
                    assertEquals(lastPosition, Long.valueOf(appender.position()));
                }
                assertEquals(i, appender.entries());
                appender.append("A");
                lastPosition = appender.position();
            }
        }

        try (LogAppender<String> appender = appender(testDirectory)) {
            assertEquals(iterations, appender.stream(Direction.FORWARD).count());
            assertEquals(iterations, appender.entries());
        }
    }


    private static String stringOfLength(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append("A");
        return sb.toString();
    }

    private void appendN(String value, long num) {
        long start = System.currentTimeMillis();

        long avg = 0;
        long lastUpdate = System.currentTimeMillis();
        long written = 0;

        for (int i = 0; i < num; i++) {
            if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                avg = (avg + written) / 2;
//                System.out.println("TOTAL WRITTEN: " + appender.entries() + " - LAST SECOND: " + written + " - AVG: " + avg);
                written = 0;
                lastUpdate = System.currentTimeMillis();
            }
            appender.append(value);
//            appender.appendAsync(value, pos -> {
//            });
            written++;
        }

        System.out.println("APPENDER_WRITE - " + appender.entries() + " IN " + (System.currentTimeMillis() - start) + "ms");
    }


    private void scanAllAssertingSameValue(String expected, Direction direction) {
        long start = System.currentTimeMillis();
        try (LogIterator<String> logIterator = appender.iterator(direction)) {

            long avg = 0;
            long lastUpdate = System.currentTimeMillis();
            long read = 0;
            long totalRead = 0;

            while (logIterator.hasNext()) {
                if (System.currentTimeMillis() - lastUpdate >= TimeUnit.SECONDS.toMillis(1)) {
                    avg = (avg + read) / 2;
                    System.out.println("TOTAL READ: " + totalRead + " - LAST SECOND: " + read + " - AVG: " + avg);
                    read = 0;
                    lastUpdate = System.currentTimeMillis();
                }
                String found = logIterator.next();
                assertEquals(expected, found);
                read++;
                totalRead++;
            }

            assertEquals(appender.entries(), totalRead);
            System.out.println("APPENDER_READ -  READ " + totalRead + " ENTRIES IN " + (System.currentTimeMillis() - start) + "ms");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static class CachedRafLogAppenderTest extends LogAppenderTest {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF_CACHED)
                    .open();
        }
    }

    public static class MMapLogAppenderTest extends LogAppenderTest {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .threadPerLevelCompaction()
                    .storageMode(StorageMode.MMAP)
                    .open();
        }
    }

    public static class RafLogAppenderTest extends LogAppenderTest {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF)
                    .open();
        }
    }


}