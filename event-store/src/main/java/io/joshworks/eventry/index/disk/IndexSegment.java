package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.midpoint.Midpoint;
import io.joshworks.eventry.index.midpoint.Midpoints;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockIterator;
import io.joshworks.fstore.log.segment.block.BlockPoller;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class IndexSegment implements Log<IndexEntry>, Index {

    BloomFilter<Long> filter;
    final Midpoints midpoints;
    private final File directory;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private final BlockSegment<IndexEntry> delegate;



    private static final double FALSE_POSITIVE_PROB = 0.01;

    IndexSegment(Storage storage,
                 IDataStream reader,
                 String magic,
                 Type type,
                 File directory,
                 Codec codec,
                 int numElements) {

        this.delegate = new BlockSegment<>(storage, reader, magic, type,  new IndexEntrySerializer(), new IndexBlockFactory(), codec,MAX_BLOCK_SIZE, this::onBlockWrite);
        this.directory = directory;
        this.midpoints = new Midpoints(directory, name());
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(Serializers.LONG));
    }

    protected synchronized void onBlockWrite(Long blockPos, Block<IndexEntry> block) {
        Midpoint head = new Midpoint(block.first(), blockPos);
        Midpoint tail = new Midpoint(block.last(), blockPos);
        midpoints.add(head, tail);
    }

    @Override
    public long append(IndexEntry data) {
        filter.add(data.stream);
        return delegate.add(data);
    }

    @Override
    public synchronized void flush() {
        if (readOnly()) {
            return;
        }
        delegate.flush();
        midpoints.write();
        filter.write();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Stream<IndexEntry> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    @Override
    public LogIterator<IndexEntry> iterator(long position, Direction direction) {
        return new BlockIterator<>(delegate.iterator(position, direction), direction);
    }

    @Override
    public LogIterator<IndexEntry> iterator(Direction direction) {
        return new BlockIterator<>(delegate.iterator(direction), direction);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public IndexEntry get(long position) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public PollingSubscriber<IndexEntry> poller(long position) {
        return new BlockPoller<>(delegate.poller(position));
    }

    @Override
    public PollingSubscriber<IndexEntry> poller() {
        return new BlockPoller<>(delegate.poller());
    }

    @Override
    public long fileSize() {
        return delegate.fileSize();
    }

    @Override
    public long logicalSize() {
        return delegate.logicalSize();
    }

    @Override
    public Set<TimeoutReader> readers() {
        return delegate.readers();
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return delegate.rebuildState(lastKnownPosition);
    }

    @Override
    public void delete() {
        delegate.delete();
        filter.delete();
        midpoints.delete();
    }

    @Override
    public void roll(int level) {
        flush();
        delegate.roll(level);
    }


    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    void newBloomFilter(long numElements) {
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(Serializers.LONG));
    }

    private boolean definitelyNotPresent(Range range) {
        return !midpoints.inRange(range) || !filter.contains(range.stream);
    }


    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction) {
        return null;
    }

    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction, Range range) {
        if (definitelyNotPresent(range)) {
            return Iterators.empty();
        }

        Midpoint lowBound = midpoints.getMidpointFor(range.start());
        if (lowBound == null) {
            return Iterators.empty();
        }

        LogIterator<IndexEntry> logIterator = iterator(lowBound.position, direction);
        return new RangeIndexEntryIterator(range, logIterator);
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction) {
        return Iterators.closeableStream(indexIterator(direction));
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction, Range range) {
        return Iterators.closeableStream(indexIterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Range range = Range.of(stream, version, version + 1);
        if (definitelyNotPresent(range)) {
            return Optional.empty();
        }

        IndexEntry start = range.start();
        Midpoint lowBound = midpoints.getMidpointFor(start);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return Optional.empty();
        }

        IndexBlock foundBlock = (IndexBlock) delegate.get(lowBound.position);
        List<IndexEntry> entries = foundBlock.entries();
        int idx = Collections.binarySearch(entries, start);
        if (idx < 0) { //if not exact match, wasn't found
            return Optional.empty();
        }
        IndexEntry found = entries.get(idx);
        if (found == null || found.stream != stream && found.version != version) { //sanity check
            throw new IllegalStateException("Inconsistent index");
        }
        return Optional.of(found);

    }

    @Override
    public int version(long stream) {
        Range range = Range.allOf(stream);
        if (definitelyNotPresent(range)) {
            return IndexEntry.NO_VERSION;
        }

        IndexEntry end = range.end();
        Midpoint lowBound = midpoints.getMidpointFor(end);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return IndexEntry.NO_VERSION;
        }

        IndexBlock foundBlock = (IndexBlock) delegate.get(lowBound.position);
        List<IndexEntry> entries = foundBlock.entries();
        int idx = Collections.binarySearch(entries, end);
        idx = idx >= 0 ? idx : Math.abs(idx) - 2;
        IndexEntry lastVersion = entries.get(idx);
        if (lastVersion.stream != stream) { //false positive on the bloom filter
            return IndexEntry.NO_VERSION;
        }
        return lastVersion.version;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void close() {
        //do not flush
        delegate.close();
    }


    private static final class RangeIndexEntryIterator implements LogIterator<IndexEntry> {

        private final IndexEntry end;
        private final IndexEntry start;
        private final LogIterator<IndexEntry> segmentIterator;
        private IndexEntry current;

        private RangeIndexEntryIterator(Range range, LogIterator<IndexEntry> logIterator) {
            this.end = range.end();
            this.start = range.start();
            this.segmentIterator = logIterator;

            //initial load skipping less than queuedTime
            while (logIterator.hasNext()) {
                IndexEntry next = logIterator.next();
                if (next.greatOrEqualsTo(start)) {
                    current = next;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = current != null && current.lessThan(end);
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public void close() {
            try {
                segmentIterator.close();
            } catch (IOException e) {
                throw RuntimeIOException.of(e);
            }
        }

        @Override
        public IndexEntry next() {
            if (current == null) {
                close();
                throw new NoSuchElementException();
            }
            IndexEntry curr = current;
            current = segmentIterator.hasNext() ? segmentIterator.next() : null;
            return curr;
        }

        @Override
        public long position() {
            return segmentIterator.position();
        }
    }

}
