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
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.serializer.ByteBufferCopy;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class IndexSegment implements Log<IndexBlock>, Index {

    BloomFilter<Long> filter;
    final Midpoints midpoints;
    final File directory;
    private final Segment<ByteBuffer> delegate;

    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private static final double FALSE_POSITIVE_PROB = 0.01;
    private final Codec codec;

    IndexSegment(Storage storage,
                        IDataStream reader,
                        String magic,
                        Type type,
                        File directory,
                        Codec codec,
                        int numElements) {
        this.codec = codec;
        this.delegate = new Segment<>(storage, new ByteBufferCopy(), reader, magic, type);
        this.directory = directory;
        this.midpoints = new Midpoints(directory, name());
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(Serializers.LONG));
    }

    protected synchronized long writeBlock() {
        IndexBlock block = (IndexBlock) currentBlock();
        if (block.isEmpty()) {
            return position();
        }

        long blockPos = super.writeBlock();

        Midpoint head = new Midpoint(block.first(), blockPos);
        Midpoint tail = new Midpoint(block.last(), blockPos);
        midpoints.add(head, tail);

        return blockPos;
    }

    @Override
    public long append(IndexBlock data) {
        for (IndexEntry entry : data.entries()) {
            filter.add(entry.stream);
        }
        return super.append(data);
    }

    @Override
    public synchronized void flush() {
        super.flush(); //flush super first, so writeBlock is called
        midpoints.write();
        filter.write();
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public Stream<IndexBlock> stream(Direction direction) {
        return null;
    }

    @Override
    public LogIterator<IndexBlock> iterator(long position, Direction direction) {
        return null;
    }

    @Override
    public LogIterator<IndexBlock> iterator(Direction direction) {
        return null;
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public Marker marker() {
        return null;
    }

    @Override
    public IndexBlock get(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<IndexBlock> poller(long position) {
        return null;
    }

    @Override
    public PollingSubscriber<IndexBlock> poller() {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public Set<TimeoutReader> readers() {
        return null;
    }

    @Override
    public SegmentState rebuildState(long lastKnownPosition) {
        return null;
    }

    @Override
    public void delete() {
        super.delete();
        filter.delete();
        midpoints.delete();
    }

    @Override
    public void roll(int level) {

    }

    @Override
    public void roll(int level, ByteBuffer footer) {

    }

    @Override
    public ByteBuffer readFooter() {
        return null;
    }

    @Override
    public boolean readOnly() {
        return false;
    }

    @Override
    public long entries() {
        return 0;
    }

    @Override
    public int level() {
        return 0;
    }

    @Override
    public long created() {
        return 0;
    }

    void newBloomFilter(long numElements) {
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(Serializers.LONG));
    }

    private boolean mightHaveEntries(Range range) {
        return midpoints.inRange(range) && filter.contains(range.stream);
    }


    @Override
    public LogIterator<IndexEntry> iterator(Direction direction, Range range) {
        if (!mightHaveEntries(range)) {
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
    public Stream<IndexEntry> stream(Direction direction, Range range) {
        return Iterators.closeableStream(iterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        Range range = Range.of(stream, version, version + 1);
        if (!mightHaveEntries(range)) {
            return Optional.empty();
        }

        IndexEntry start = range.start();
        Midpoint lowBound = midpoints.getMidpointFor(start);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return Optional.empty();
        }

        IndexBlock block = (IndexBlock) getBlock(lowBound.position);
        List<IndexEntry> entries = block.entries();
        int idx = Collections.binarySearch(entries, start);
        if(idx < 0) { //if not exact match, wasn't found
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
        if (!mightHaveEntries(range)) {
            return IndexEntry.NO_VERSION;
        }

        IndexEntry end = range.end();
        Midpoint lowBound = midpoints.getMidpointFor(end);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return IndexEntry.NO_VERSION;
        }

        IndexBlock block = (IndexBlock) getBlock(lowBound.position);
        List<IndexEntry> entries = block.entries();
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
        return super.toString();
    }

    @Override
    public void close() throws IOException {

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
            if(!hasNext) {
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
