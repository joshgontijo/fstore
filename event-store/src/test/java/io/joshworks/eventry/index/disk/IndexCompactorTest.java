package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.eventry.stream.StreamMetadata.STREAM_ACTIVE;
import static org.junit.Assert.assertEquals;

public class IndexCompactorTest {

    private File indexDir;

    private File segmentFile1;
    private File segmentFile2;
    private File outputFile;

    private IndexSegment segment1;
    private IndexSegment segment2;
    private IndexSegment output;

    @Before
    public void setUp() {
        indexDir = FileUtils.testFolder();
        segmentFile1 = new File(indexDir, "segment1");
        segmentFile2 = new File(indexDir, "segment2");
        outputFile = new File(indexDir, "output");
        segment1 = open(segmentFile1);
        segment2 = open(segmentFile2);
        output = open(outputFile);
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(segment1);
        IOUtils.closeQuietly(segment2);
        IOUtils.closeQuietly(output);
        Files.delete(segmentFile1.toPath());
        Files.delete(segmentFile2.toPath());
        Files.delete(outputFile.toPath());
    }

    private IndexSegment open(File location) {
        return new IndexSegment(
                location,
                StorageMode.RAF,
                Size.MB.of(100),
                new BufferPool(),
                WriteMode.LOG_HEAD,
                indexDir,
                new SnappyCodec(),
                0.01,
                Memory.PAGE_SIZE,
                100000);
    }

    private static StreamMetadata defaultMetadata(long stream) {
        return new StreamMetadata("stream-" + stream, stream, 0, 0, 0, NO_TRUNCATE, Map.of(), Map.of(), STREAM_ACTIVE);
    }

    @Test
    public void negative_position_removes_the_entry_when_merging() {

        long stream = 123;

        segment1.append(IndexEntry.of(stream, 0, 0));
        segment1.append(IndexEntry.of(stream, 1, 0));
        segment1.append(IndexEntry.of(stream, 2, 0));

        segment1.flush();

        segment2.append(IndexEntry.of(stream, 0, -1));
        segment2.append(IndexEntry.of(stream, 1, -1));
        segment2.flush();


        var compactor = new IndexCompactor(IndexCompactorTest::defaultMetadata);
        compactor.merge(List.of(segment1, segment2), output);

        output.flush();

        print(output);
        assertEquals(1, output.entries());

        IndexEntry entry = output.iterator(Direction.FORWARD).stream().collect(Collectors.toList()).get(0);
        assertEquals(2, entry.version);

    }

    private static void print(IndexSegment segment) {
        segment.iterator(Direction.FORWARD).forEachRemaining(System.out::println);
    }
}