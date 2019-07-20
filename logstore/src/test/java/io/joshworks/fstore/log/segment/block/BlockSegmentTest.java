package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class BlockSegmentTest {

    protected BlockSegment<String> segment;
    private File testFile;

    private static final double CHECKSUM_PROB = 1;
    private static final int READ_PAGE_SIZE = Memory.PAGE_SIZE;

    BlockSegment<String> open(File file) {
        return new BlockSegment<>(
                file, StorageMode.RAF,
                Size.MB.of(10),
                new BufferPool(false),
                WriteMode.LOG_HEAD,
                Serializers.STRING,
                VLenBlock.factory(),
                new SnappyCodec(),
                Memory.PAGE_SIZE,
                CHECKSUM_PROB,
                READ_PAGE_SIZE);
    }

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
    public void entries_returns_total_entries_of_all_blocks() {
        segment.append("a");
        segment.append("b");

        assertEquals(2, segment.entries());
    }

    @Test
    public void entries_is_correct_after_reopening_segment() {
        segment.append("a");
        segment.append("b");

        segment.close();
        segment = open(testFile);
        assertEquals(2, segment.entries());
    }

    @Test
    public void entries_is_correct_after_rolling_segment() {
        segment.append("a");
        segment.append("b");

        segment.roll(1, false);
        segment.close();
        segment = open(testFile);
        assertEquals(2, segment.entries());
    }

    @Test
    public void flush_writes_block() {
        segment.append("a");
        segment.flush();

        segment.close();
        segment = open(testFile);
        assertEquals(1, segment.entries());
    }

    @Test
    public void roll_writes_block() {
        segment.append("a");
        segment.roll(1, false);

        segment.close();
        segment = open(testFile);
        assertEquals(1, segment.entries());
    }

    @Test
    public void when_new_block_doesnt_fit_in_the_segment_append_returns_EOF() {
        long bPos;
        do {
            bPos = segment.append("a");
        } while (bPos != Storage.EOF);

        assertEquals(Storage.EOF, segment.append("a"));
    }

    @Test
    public void uncompressed_size_is_the_approximation_of_block_size() {
        for (int i = 0; i < 1000000; i++) {
            segment.append("a");
        }
        assertEquals(segment.blocks() * segment.blockSize(), segment.uncompressedSize());
    }

    @Test
    public void uncompressed_is_restored_when_segment_is_reopened() {
        for (int i = 0; i < 1000000; i++) {
            segment.append("a");
        }

        segment.flush();
        long uncompressedSize = segment.uncompressedSize();

        segment.close();
        segment = open(testFile);
        assertEquals(uncompressedSize, segment.uncompressedSize());
    }

    @Test
    public void uncompressed_is_restored_when_segment_is_rolled() {
        long bPos;
        do {
            bPos = segment.append("a");
        } while (bPos != Storage.EOF);

        long uncompressedSize = segment.uncompressedSize();

        segment.roll(1, false);
        segment.close();
        segment = open(testFile);
        assertEquals(uncompressedSize, segment.uncompressedSize());
    }
}