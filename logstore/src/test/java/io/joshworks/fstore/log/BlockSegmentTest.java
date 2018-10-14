package io.joshworks.fstore.log;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.serializer.Serializers;
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

public class BlockSegmentTest extends SegmentTest {

    protected final int blockSize = 4096;
    protected static final int START = 0;

    @Override
    Log<String> open(File file) {
        return new BlockSegment<>(
                new RafStorage(file, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE),
                Serializers.STRING, new DataStream(START), "magic", Type.LOG_HEAD,
                VLenBlock.factory(), new SnappyCodec(), blockSize);
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
    public void entryIdx_returns_correct_position() {
        long logPos = LogAppender.MAX_SEGMENT_ADDRESS;
        for (int blocEntryIdx = 0; blocEntryIdx < LogAppender.MAX_BLOCK_VALUE; blocEntryIdx++) {
            long position = withBlockIndex(blocEntryIdx, logPos);
            int entryIdx = entryIdx(position);
            assertEquals(blocEntryIdx, entryIdx);
        }
    }

    @Test
    public void blockPosition_returns_correct_position() {
        long blockIdx = LogAppender.MAX_BLOCK_VALUE;

        long position = withBlockIndex(blockIdx, 0);
        assertEquals(0, blockPosition(position));

        position = withBlockIndex(blockIdx, LogAppender.MAX_SEGMENT_ADDRESS);
        assertEquals(LogAppender.MAX_SEGMENT_ADDRESS, blockPosition(position));
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

            assertEquals("a", iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(pos2, iterator.position());

            assertEquals("b", iterator.next());

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
            assertEquals("a", iterator.next());
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
            assertEquals("b", iterator.next());

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
            assertEquals("b", iterator.next());
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
            assertEquals("a", iterator.next());
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

    @Test(expected = IllegalArgumentException.class)
    public void getBlock_throws_exception_of_no_block_is_present() {
        ((BlockSegment<String>) segment).getBlock(BlockSegment.START);
    }

    @Test
    public void getBlock_returns_correct_block() {
        long position = segment.append("a");
        segment.flush();

        Block<String> found = ((BlockSegment<String>) segment).getBlock(position);
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
