package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.midpoint.Midpoint;
import io.joshworks.eventry.index.midpoint.Midpoints;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockIterator;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexSegment implements Log<IndexEntry> {

    private static final double FALSE_POSITIVE_PROB = 0.01;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private final AtomicBoolean closed = new AtomicBoolean();
    final Midpoints midpoints;
    private final File directory;
    BloomFilter<Long> filter;

    private final BlockSegment<IndexEntry> delegate;

    IndexSegment(Storage storage,
                 IDataStream reader,
                 String magic,
                 Type type,
                 File directory,
                 Codec codec,
                 int numElements) {

        this.delegate = new BlockSegment<>(storage, reader, magic, type, new IndexEntrySerializer(), new IndexBlockFactory(), codec, MAX_BLOCK_SIZE, this::onBlockWrite);
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
    public SegmentIterator<IndexEntry> iterator(long position, Direction direction) {
        return new BlockIterator<>(delegate.iterator(position, direction), direction);
    }

    @Override
    public SegmentIterator<IndexEntry> iterator(Direction direction) {
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
    public long fileSize() {
        return delegate.fileSize();
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
        delegate.roll(level);
        midpoints.write();
        filter.write();
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

    private boolean definitelyNotPresent(long stream, int version) {
        Range range = Range.of(stream, version, version + 1);
        return !midpoints.inRange(range) || !filter.contains(stream);
    }

    public LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
        if (definitelyNotPresent(range)) {
            return Iterators.empty();
        }

        Midpoint lowBound = midpoints.getMidpointFor(range.start());
        if (lowBound == null) {
            return Iterators.empty();
        }
        int firstVersion = firstVersionOf(range.start().stream);
        int lastVersion = lastVersionOf(range.end().stream);

        int start = Math.max(range.start().version, firstVersion);
        int end = Math.min(range.end().version, lastVersion);

        return new RangeIndexEntryIterator(direction, range, start, end);
    }

    public List<IndexEntry> readBlockEntries(long stream, int version) {
        if (definitelyNotPresent(stream, version)) {
            return Collections.emptyList();
        }

        IndexEntry start = IndexEntry.of(stream, version, 0);
        Midpoint lowBound = midpoints.getMidpointFor(start);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return Collections.emptyList();
        }
        IndexBlock foundBlock = (IndexBlock) delegate.get(lowBound.position);
        List<IndexEntry> entries = foundBlock.entries();
        int idx = Collections.binarySearch(entries, start);
        if (idx < 0) { //if not exact match, wasn't found
            return Collections.emptyList();
        }
        return foundBlock.entries();
    }

    public Optional<IndexEntry> get(long stream, int version) {
        if (definitelyNotPresent(stream, version)) {
            return Optional.empty();
        }

        IndexEntry start = IndexEntry.of(stream, version, 0);
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

    int lastVersionOf(long stream) {
        Range range = Range.anyOf(stream);
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

    int firstVersionOf(long stream) {
        Range range = Range.anyOf(stream);
        if (definitelyNotPresent(range)) {
            return IndexEntry.NO_VERSION;
        }

        IndexEntry start = range.start();
        Midpoint lowBound = midpoints.getMidpointFor(start);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return IndexEntry.NO_VERSION;
        }

        IndexBlock foundBlock = (IndexBlock) delegate.get(lowBound.position);
        List<IndexEntry> entries = foundBlock.entries();
        int idx = Collections.binarySearch(entries, start);
        idx = idx >= 0 ? idx : Math.abs(idx) - 1;
        if(idx >= entries.size()) {
            return IndexEntry.NO_VERSION;
        }
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
        closed.set(true);
    }


    private final class RangeIndexEntryIterator implements SegmentIterator<IndexEntry> {

        private Direction direction;
        private final Range range;
        private final int startVersion;
        private final int endVersion;

        private long lastBlockPos;
        private int lastReadVersion;
        private Queue<IndexEntry> entries = new LinkedList<>();
        private SegmentIterator<Block<IndexEntry>> lock;

        private RangeIndexEntryIterator(Direction direction, Range range, int startVersion, int endVersion) {
            this.direction = direction;
            this.range = range;
            this.startVersion = startVersion;
            this.endVersion = endVersion;
            this.lastReadVersion = Direction.FORWARD.equals(direction) ? startVersion - 1 : endVersion + 1;
            this.lock = IndexSegment.this.delegate.iterator(Direction.FORWARD);
        }

        private void fetchEntries() {
            int nextVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
            if (nextVersion < startVersion || nextVersion > endVersion) {
                return;
            }

            IndexEntry key = IndexEntry.of(range.stream, nextVersion, 0);
            Midpoint lowBound = midpoints.getMidpointFor(key);
            if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
                return;
            }
            IndexBlock foundBlock = (IndexBlock) delegate.get(lowBound.position);
            this.lastBlockPos = lowBound.position;
            List<IndexEntry> blockEntries = foundBlock.entries();
            if (Direction.BACKWARD.equals(direction)) {
                Collections.reverse(blockEntries);
            }

            for (IndexEntry entry : blockEntries) {
                if (range.match(entry) && !hasRead(entry)) {
                    this.entries.add(entry);
                }
            }
        }

        private boolean hasRead(IndexEntry entry) {
            if (Direction.FORWARD.equals(direction)) {
                return entry.version <= lastReadVersion;
            }
            return entry.version >= lastReadVersion;
        }

        @Override
        public IndexEntry next() {
            if (!hasNext()) {
                return null;
            }
            IndexEntry polled = entries.poll();
            if (polled == null) {
                throw new NoSuchElementException(); //should never happen
            }
            lastReadVersion = polled.version;
            return polled;
        }

        @Override
        public boolean hasNext() {
            if (!entries.isEmpty()) {
                return true;
            }
            fetchEntries();
            return !entries.isEmpty();
        }

        @Override
        public void close() {
            entries.clear();
            IOUtils.closeQuietly(lock);
        }

        @Override
        public long position() {
            return lastBlockPos;
        }

        @Override
        public boolean endOfLog() {
            return IndexSegment.this.readOnly() && !hasNext();
        }
    }

}
