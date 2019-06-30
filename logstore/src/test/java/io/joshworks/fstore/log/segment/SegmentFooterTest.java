package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class SegmentFooterTest {

    protected static final double CHECKSUM_PROB = 1;
    protected static final int SEGMENT_SIZE = Size.KB.intOf(128);
    protected static final int MAX_ENTRY_SIZE = SEGMENT_SIZE;
    private static final int BUFFER_SIZE = Memory.PAGE_SIZE;

    protected FooterSegment segment;
    private File testFile;

    abstract FooterSegment open(File file);

    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        segment = open(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(segment);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void footer_returns_all_items() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(segment.footerItems.toArray(), read.toArray());
    }

    @Test
    public void can_read_footer_after_reopening() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.close();
        segment = open(testFile);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(segment.footerItems.toArray(), read.toArray());
    }

    @Test
    public void can_read_data_with_footer() {

        long itemPos = segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);

        assertEquals("a", segment.get(itemPos));
    }

    @Test
    public void can_read_data_with_footer_after_reopening() {

        long itemPos = segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.close();
        segment = open(testFile);

        assertEquals("a", segment.get(itemPos));
    }

    @Test
    public void can_iterate_data_with_footer() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);

        assertEquals("a", segment.iterator(Direction.FORWARD).next());
    }

    @Test
    public void can_iterate_data_with_footer_after_reopening() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.close();
        segment = open(testFile);

        assertEquals("a", segment.iterator(Direction.FORWARD).next());
    }

    @Test
    public void iterator_does_not_advance_to_footer_section() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void iterator_does_not_advance_to_footer_section_after_reopening() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.close();
        segment = open(testFile);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void large_footer_can_be_read() {

        segment.append("a");
        int numFooterItems = 10000000;
        for (int i = 0; i < numFooterItems; i++) {
            segment.footerItems.add((long) i);
        }

        segment.roll(1);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void footer_is_not_deleted_when_segment_is_truncated() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.truncate();

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(segment.footerItems.toArray(), read.toArray());
    }

    @Test
    public void footer_is_not_deleted_when_segment_is_truncated_after_reopened() {

        segment.append("a");
        segment.footerItems.add(0L);
        segment.footerItems.add(1L);
        segment.footerItems.add(2L);
        segment.footerItems.add(3L);

        segment.roll(1);
        segment.truncate();
        segment.close();
        segment = open(testFile);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(segment.footerItems.toArray(), read.toArray());
    }

    public static class CachedSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(
                    file,
                    StorageMode.RAF_CACHED,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE),
                    "magic",
                    WriteMode.LOG_HEAD);
        }
    }

    public static class MMapSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(
                    file,
                    StorageMode.MMAP,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE), "magic", WriteMode.LOG_HEAD);
        }
    }

    public static class RafSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(
                    file,
                    StorageMode.RAF,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE),
                    WriteMode.LOG_HEAD);
        }
    }

    private static class FooterSegment extends Segment<String> {

        public List<Long> footerItems = new ArrayList<>();

        public FooterSegment(File file, StorageMode storageMode, long segmentDataSize, Serializer<String> serializer, IDataStream dataStream, WriteMode writeMode) {
            super(file, storageMode, segmentDataSize, serializer, dataStream, writeMode);
        }

        @Override
        public void writeFooter(FooterWriter footer) {
            for (Long footerItem : footerItems) {
                footer.write(ByteBuffer.allocate(Long.BYTES).putLong(footerItem).flip());
            }
        }

        private List<Long> readAllFooterItems() {
            FooterReader footerReader = readFooter();
            ByteBuffer footerData = ByteBuffer.allocate((int) footerReader.length());
            footerReader.read(footerReader.start(), footerData);
            footerData.flip();

            List<Long> items = new ArrayList<>();
            while (footerData.hasRemaining()) {
                items.add(footerData.getLong());
            }
            return items;
        }
    }

}
