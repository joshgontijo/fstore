package io.joshworks.es.compaction;

import io.joshworks.es.Event;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.log.LogSegment;
import io.joshworks.es.log.SegmentIterator;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.mmap.MappedFile;
import io.joshworks.fstore.core.seda.TimeWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * Sorts an entire log by reading its contents keys into memory then flushing offsets to disk
 * Shouldn't take too much memory but level zero segments must not be too large
 */
public class SortingSegment {

    private static final Logger log = LoggerFactory.getLogger(SortingSegment.class);

    private final LogSegment segment;
    private MappedFile mf;
    private final File tmpFile;
    private List<IndexEntry> entries = new ArrayList<>();

    private SortingSegment(LogSegment segment) {
        try {
            this.segment = segment;
            File folder = segment.file().toPath().getParent().toFile();
            String fileName = segment.name() + ".keys";
            this.tmpFile = new File(folder, fileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SortingSegment sort(LogSegment segment) {
        SortingSegment ss = new SortingSegment(segment);
        ss.sort();
        return ss;
    }

    private void sort() {
        log.info("Sorting log {} into {}", segment.name(), tmpFile.getName());
        TimeWatch watch = TimeWatch.start();

        SegmentIterator it = segment.iterator();
        while (it.hasNext()) {
            long offset = it.offset();
            ByteBuffer entry = it.next();
            long stream = Event.stream(entry);
            int version = Event.version(entry);
            entries.add(new IndexEntry(stream, version, offset));
        }
        entries.sort(IndexEntry::compare);

        int size = entries.size();
        mf = MappedFile.create(tmpFile, size * Long.BYTES);

        for (IndexEntry entry : entries) {
            mf.putLong(entry.logAddress());
        }

        entries = null;
        mf.flush();

        log.info("Sorted {} entries from {} in {}ms, file size: {}", size, segment.name(), watch.time(), mf.capacity());
    }

    public void delete() {
        log.info("Deleting sorted keys {}", tmpFile.getName());
        mf.delete();
    }

    public Iterator<ByteBuffer> iterator() {
        return new SortedIterator(segment, mf);
    }

    private static class SortedIterator implements Iterator<ByteBuffer> {

        private final LogSegment delegate;
        private final MappedFile mf;
        private final int entries;
        private int idx = 0;

        private ByteBuffer buffer = Buffers.allocate(1024, false);

        private SortedIterator(LogSegment delegate, MappedFile mf) {
            this.delegate = delegate;
            this.mf = mf;
            this.entries = mf.capacity() / Long.BYTES;
        }

        @Override
        public boolean hasNext() {
            return idx < entries;
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long offset = mf.getLong(idx * Long.BYTES);
            idx++;
            read(offset);
            return buffer;
        }

        private int read(long offset) {
            buffer.clear();
            int read = delegate.read(buffer, offset);
            if (read < Integer.BYTES) {
                throw new RuntimeIOException("Failed to read entry at position " + offset);
            }
            buffer.flip();
            int recSize = Event.sizeOf(buffer);
            if (!Event.isValid(buffer) && recSize > buffer.capacity()) {
                buffer = Buffers.allocate(buffer.capacity() + recSize, false);
                return read(offset);
            }
            Buffers.offsetLimit(buffer, recSize);
            return recSize;
        }

    }


}
