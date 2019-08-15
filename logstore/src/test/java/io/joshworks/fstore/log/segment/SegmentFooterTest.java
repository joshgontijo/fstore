package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.core.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class SegmentFooterTest {

    public static final int MAX_ENTRY_SIZE = Size.MB.ofInt(1);
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

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(footerData.toArray(), read.toArray());
    }

    @Test
    public void can_read_footer_after_reopening() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);
        segment.close();
        segment = open(testFile);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(footerData.toArray(), read.toArray());
    }

    @Test
    public void can_read_data_with_footer() {

        List<Long> footerData = footerData();
        long itemPos = segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);

        assertEquals("a", segment.get(itemPos));
    }

    @Test
    public void can_read_data_with_footer_after_reopening() {

        List<Long> footerData = footerData();
        long itemPos = segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);
        segment.close();
        segment = open(testFile);

        assertEquals("a", segment.get(itemPos));
    }

    @Test
    public void can_iterate_data_with_footer() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);

        assertEquals("a", segment.iterator(Direction.FORWARD).next());
    }

    @Test
    public void can_iterate_data_with_footer_after_reopening() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);
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

        segment.roll(1, false);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void iterator_does_not_advance_to_footer_section_after_reopening() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, false);
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
        int numFooterItems = (MAX_ENTRY_SIZE - RecordHeader.HEADER_OVERHEAD) / Long.BYTES;
        for (long i = 0; i < numFooterItems; i++) {
            segment.footerItems.add(i);
        }

        segment.roll(1, false);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test(expected = RuntimeException.class)
    public void cannot_write_footer_entry_bigger_than_MAX_ENTRY_SIZE() {
        segment.append("a");
        int numFooterItems = (MAX_ENTRY_SIZE + 1) / Long.BYTES;
        for (long i = 0; i < numFooterItems; i++) {
            segment.footerItems.add(i);
        }

        segment.roll(1, false);
    }

    @Test
    public void footer_is_not_deleted_when_segment_is_trimmed() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, true);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(footerData.toArray(), read.toArray());
    }

    @Test
    public void footer_is_not_deleted_when_segment_is_trimmed_after_reopened() {

        List<Long> footerData = footerData();
        segment.append("a");
        segment.footerItems.addAll(footerData);

        segment.roll(1, true);
        segment.close();
        segment = open(testFile);

        List<Long> read = segment.readAllFooterItems();
        assertArrayEquals(footerData.toArray(), read.toArray());
    }

    private List<Long> footerData() {
        return Arrays.asList(1L, 2L, 3L, 4L);
    }

    public static class CachedSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(file, StorageMode.RAF_CACHED);
        }
    }

    public static class MMapSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(file, StorageMode.MMAP);
        }
    }

    public static class RafSegmentTest extends SegmentFooterTest {

        @Override
        FooterSegment open(File file) {
            return new FooterSegment(file, StorageMode.RAF);
        }
    }

    private static class FooterSegment implements Closeable {

        private static final String FOOTER_ITEM_NAME = "ITEMS";
        private static Serializer<List<Long>> listSerializer = Serializers.listSerializer(Serializers.LONG);
        private List<Long> footerItems = new ArrayList<>();

        private final Segment<String> delegate;

        public FooterSegment(File file, StorageMode storageMode) {
            this.delegate = new Segment<>(file, storageMode, Size.KB.ofInt(128), Serializers.VSTRING, new ThreadLocalBufferPool(MAX_ENTRY_SIZE), WriteMode.LOG_HEAD, 1, Memory.PAGE_SIZE, List::size, this::writeFooter);
        }

        public void writeFooter(FooterWriter footer) {
            footer.write(FOOTER_ITEM_NAME, footerItems, listSerializer);
        }

        private List<Long> readAllFooterItems() {
            FooterReader reader = delegate.footerReader();
            return reader.read(FOOTER_ITEM_NAME, listSerializer);
        }

        public long append(String data) {
            return delegate.append(data);
        }

        @Override
        public void close() {
            delegate.close();
        }

        public void roll(int level, boolean trim) {
            delegate.roll(level, trim);
        }

        public SegmentIterator<String> iterator(Direction direction) {
            return delegate.iterator(direction);
        }

        public String get(long pos) {
            return delegate.get(pos);
        }
    }

}
