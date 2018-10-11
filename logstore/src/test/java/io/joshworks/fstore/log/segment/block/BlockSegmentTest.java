package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.joshworks.fstore.log.segment.block.BlockSegment.blockPosition;
import static io.joshworks.fstore.log.segment.block.BlockSegment.entryIdx;
import static io.joshworks.fstore.log.segment.block.BlockSegment.withBlockIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BlockSegmentTest {

    private BlockSegment<String> segment;
    private File testFile;
    private final int blockSize = 4096;

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        segment = new BlockSegment<>(
                new RafStorage(testFile, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE),
                Serializers.STRING,
                new DataStream(),
                "abc",
                Type.LOG_HEAD,
                VLenBlock.factory(),
                Codec.noCompression(),
                blockSize);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(segment);
        Utils.tryDelete(testFile);
    }

    @Test
    public void block_get() {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");

        segment.flush();

        assertEquals("a", segment.get(pos1));
        assertEquals("b", segment.get(pos2));
    }

    @Test
    public void block_iterator() {
        segment.append("a");
        segment.append("b");

        segment.flush();

        List<String> found = segment.stream(Direction.FORWARD).collect(Collectors.toList());
        assertEquals(2, found.size());

        assertEquals("a", found.get(0));
        assertEquals("b", found.get(1));
    }

    @Test
    public void position() {
        for (int segPos = 0; segPos < LogAppender.MAX_SEGMENT_ADDRESS; segPos++) {
            for (int blocEntryIdx = 0; blocEntryIdx < LogAppender.MAX_BLOCK_ENTRIES; blocEntryIdx++) {

                long position = withBlockIndex(blocEntryIdx, segPos);
                int entryIdx = entryIdx(position);
                long pos = blockPosition(position);

                System.out.println(Long.toBinaryString(pos));
                System.out.println(Long.toBinaryString(entryIdx));
                System.out.println(Long.toBinaryString(position));

                assertEquals("[" + segPos + "][" + blocEntryIdx + "]", blocEntryIdx, entryIdx);
                assertEquals("[" + segPos + "][" + blocEntryIdx + "]", segPos, pos);
            }
        }
    }

    @Test
    public void get_and_append_position_are_the_same() {

        long logPos = segment.position();
        long pos1 = segment.append("a");
        assertEquals(pos1, logPos);

        logPos = segment.position();
        long pos2 = segment.append("b");
        assertEquals(pos2, logPos);

        logPos = segment.position();
        long pos3 = segment.append("c");
        assertEquals(pos3, logPos);
    }

    @Test
    public void block_forward_iterator_return_correct_position() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");

        segment.flush();

        long pos3 = segment.position();

        try (LogIterator<String> iterator = segment.iterator(Direction.FORWARD)) {
            assertTrue(iterator.hasNext());
            assertEquals(pos1, iterator.position());

            assertEquals(Integer.valueOf(1), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(pos2, iterator.position());

            assertEquals(Integer.valueOf(2), iterator.next());

            assertFalse(iterator.hasNext());
            assertEquals(pos3, iterator.position());
        }
    }

    @Test
    public void block_forward_iterator_starts_from_position_at_beginning_of_block() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");
        segment.flush();

        long pos3 = segment.position();

        try (LogIterator<String> iterator = segment.iterator(pos1, Direction.FORWARD)) {
            assertTrue(iterator.hasNext());
            assertEquals(pos1, iterator.position());
            assertEquals(Integer.valueOf(1), iterator.next());
        }

    }

    @Test
    public void block_forward_iterator_with_position_starting_at_the_middle_of_block() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");
        segment.flush();

        long pos3 = segment.position();

        try (LogIterator<String> iterator = segment.iterator(pos2, Direction.FORWARD)) {
            assertTrue(iterator.hasNext());
            assertEquals(pos2, iterator.position());
            assertEquals(Integer.valueOf(2), iterator.next());

            assertEquals(pos3, iterator.position());
            assertFalse(iterator.hasNext());
        }
    }


    @Test
    public void block_backward_iterator_starts_from_block_end_position() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");
        segment.flush();

        long pos3 = segment.position();
        try (LogIterator<String> iterator = segment.iterator(pos3, Direction.BACKWARD)) {

            assertEquals(pos3, iterator.position());

            assertTrue(iterator.hasNext());
            assertEquals(Integer.valueOf(2), iterator.next());
            assertEquals(pos2, iterator.position());

            assertTrue(iterator.hasNext());
        }

    }

    @Test
    public void block_backward_iterator_starts_from_position_in_middle_of_block() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");
        segment.flush();

        long pos4 = segment.position();

        try (LogIterator<String> iterator = segment.iterator(pos2, Direction.BACKWARD)) {
            assertEquals(pos2, iterator.position());
            assertTrue(iterator.hasNext());
            assertEquals(Integer.valueOf(1), iterator.next());
            assertEquals(pos1, iterator.position());
            assertFalse(iterator.hasNext());
        }

    }

    @Test
    public void block_backward_iterator_return_correct_position() throws IOException {
        long pos1 = segment.append("a");
        long pos2 = segment.append("b");

        segment.flush();

        long pos3 = segment.position();

        try (LogIterator<String> iterator = segment.iterator(Direction.BACKWARD)) {
            assertEquals(pos3, iterator.position());

            iterator.next();
            assertEquals(pos2, iterator.position());

            iterator.next();
            assertEquals(pos1, iterator.position());
        }
    }

    @Test
    public void entries_returns_flushed_blocks() {
        assertEquals(0, segment.entries());
        segment.append("a");
        segment.flush();
        assertEquals(1, segment.entries());
    }

    @Test
    public void stream_returns_all_data() {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).mapToObj(String::valueOf).forEach(segment::append);

        segment.flush();

        long count = segment.stream(Direction.FORWARD).count();
        assertEquals(entriesPerBlock, count);
    }

    @Test(expected = IllegalStateException.class)
    public void getBlock_throws_exception_of_no_block_is_present() {
        segment.getBlock(Log.START);
    }

    @Test
    public void getBlock_returns_correct_block() {
        long position = segment.append("a");
        segment.flush();

        Block<String> found = segment.getBlock(position);
        assertNotNull(found);
        assertEquals(1, found.entryCount());
        assertEquals("a", found.get(0));
    }

    @Test
    public void poller_take_returns_all_persisted_data() throws IOException, InterruptedException {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).mapToObj(String::valueOf).forEach(segment::append);

        segment.flush();

        try (PollingSubscriber<String> poller = segment.poller()) {
            for (int i = 0; i < entriesPerBlock; i++) {
                String val = poller.take();
                assertEquals(String.valueOf(i), val);
            }
        }
    }

    @Test
    public void poller_poll_returns_all_persisted_data() throws IOException, InterruptedException {
        int entriesPerBlock = blockSize / Integer.BYTES;
        IntStream.range(0, entriesPerBlock).mapToObj(String::valueOf).forEach(segment::append);

        segment.flush();

        try (PollingSubscriber<String> poller = segment.poller()) {
            for (int i = 0; i < entriesPerBlock; i++) {
                String val = poller.poll();
                assertEquals(String.valueOf(i), val);
            }
        }
    }

    @Test
    public void poller_poll_doesnt_return_non_persisted_data() throws IOException, InterruptedException {
        segment.append("a");
        segment.append("b");

        try (PollingSubscriber<String> poller = segment.poller()) {
            String poll = poller.poll();
            assertNull(poll);
        }
    }

    @Test
    public void endOfLog_only_when_queue_is_empty_and_closed_was_called() throws IOException, InterruptedException {
        segment.append("a");
        segment.append("b");

        PollingSubscriber<String> poller = segment.poller();
        poller.close();
        assertFalse(poller.endOfLog());
        poller.poll();
        assertFalse(poller.endOfLog());
        poller.poll();
        assertFalse(poller.endOfLog());

    }
}