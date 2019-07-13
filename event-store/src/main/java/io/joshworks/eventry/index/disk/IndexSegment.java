package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.midpoint.Midpoint;
import io.joshworks.eventry.index.midpoint.Midpoints;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.filter.BloomFilter;
import io.joshworks.fstore.core.filter.BloomFilterHasher;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockIterator;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexSegment implements Log<IndexEntry> {

    private static final double FALSE_POSITIVE_PROB = 0.01;
    private static final int MAX_BLOCK_SIZE = Memory.PAGE_SIZE;

    private final AtomicBoolean closed = new AtomicBoolean();
    final Midpoints midpoints;
    private final File directory;
    private final IndexEntrySerializer indexEntrySerializer = new IndexEntrySerializer();
    BloomFilter<Long> filter;

    private final BlockSegment<IndexEntry> delegate;

    IndexSegment(File file,
                 StorageMode storageMode,
                 long dataLength,
                 BufferPool bufferPool,
                 WriteMode mode,
                 File directory,
                 Codec codec,
                 double checksumProb,
                 int readPageSize,
                 int numElements) {
        this.delegate = new BlockSegment<>(file, storageMode, dataLength, bufferPool, mode, indexEntrySerializer, new IndexBlockFactory(), codec, MAX_BLOCK_SIZE, checksumProb, readPageSize, this::onBlockWrite, (p,b) -> {}, null);
        this.directory = directory;
        this.midpoints = new Midpoints(directory, name());
        this.filter = BloomFilter.openOrCreate(directory, name(), numElements, FALSE_POSITIVE_PROB, BloomFilterHasher.murmur64(Serializers.LONG));
    }

    protected synchronized void onBlockWrite(Long blockPos, Block block) {
        IndexEntry first = indexEntrySerializer.fromBytes(block.first());
        IndexEntry last = indexEntrySerializer.fromBytes(block.last());
        Midpoint head = new Midpoint(first, blockPos);
        Midpoint tail = new Midpoint(last, blockPos);
        midpoints.add(head, tail);
    }

    @Override
    public long append(IndexEntry data) {
        long pos = delegate.append(data);
        if (pos != Storage.EOF) {
            filter.add(data.stream);
        }
        return pos;
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
        return new BlockIterator<>(indexEntrySerializer, delegate.blockIterator(position, direction), direction);
    }

    @Override
    public SegmentIterator<IndexEntry> iterator(Direction direction) {
        return new BlockIterator<>(indexEntrySerializer, delegate.blockIterator(direction), direction);
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
    public long logSize() {
        return delegate.logSize();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
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
    public boolean closed() {
        return closed.get();
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
    public void trim() {
        delegate.trim();
    }

    @Override
    public long uncompressedSize() {
        return delegate.uncompressedSize();
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


    //TODO this should be for index internal use only. Use 'readBlockEntries'
    LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
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

        SegmentIterator<Block> lock = delegate.blockIterator(Direction.FORWARD);
        return new RangeIndexEntryIterator(this, direction, range, start, end, lock, delegate, indexEntrySerializer);
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
        IndexBlock foundBlock = (IndexBlock) delegate.getBlock(lowBound.position);
        List<IndexEntry> entries = foundBlock.deserialize(indexEntrySerializer);
        int idx = Collections.binarySearch(entries, start);
        if (idx < 0) { //if not exact match, wasn't found
            return Collections.emptyList();
        }
        return entries;
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

        IndexBlock foundBlock = (IndexBlock) delegate.getBlock(lowBound.position);
        List<IndexEntry> entries = foundBlock.deserialize(indexEntrySerializer);
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
            return EventRecord.NO_VERSION;
        }

        IndexEntry end = range.end();
        Midpoint lowBound = midpoints.getMidpointFor(end);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return EventRecord.NO_VERSION;
        }

        IndexBlock foundBlock = (IndexBlock) delegate.getBlock(lowBound.position);
        List<IndexEntry> entries = foundBlock.deserialize(indexEntrySerializer);
        int idx = Collections.binarySearch(entries, end);
        idx = idx >= 0 ? idx : Math.abs(idx) - 2;
        if (idx < 0) {
            return EventRecord.NO_VERSION;
        }
        IndexEntry lastVersion = entries.get(idx);
        if (lastVersion.stream != stream) { //false positive on the bloom filter
            return EventRecord.NO_VERSION;
        }
        return lastVersion.version;
    }

    int firstVersionOf(long stream) {
        Range range = Range.anyOf(stream);
        if (definitelyNotPresent(range)) {
            return EventRecord.NO_VERSION;
        }

        IndexEntry start = range.start();
        Midpoint lowBound = midpoints.getMidpointFor(start);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return EventRecord.NO_VERSION;
        }

        IndexBlock foundBlock = (IndexBlock) delegate.getBlock(lowBound.position);
        List<IndexEntry> entries = foundBlock.deserialize(indexEntrySerializer);
        int idx = Collections.binarySearch(entries, start);
        idx = idx >= 0 ? idx : Math.abs(idx) - 1;
        if (idx >= entries.size()) {
            return EventRecord.NO_VERSION;
        }
        IndexEntry lastVersion = entries.get(idx);
        if (lastVersion.stream != stream) { //false positive on the bloom filter
            return EventRecord.NO_VERSION;
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
}
