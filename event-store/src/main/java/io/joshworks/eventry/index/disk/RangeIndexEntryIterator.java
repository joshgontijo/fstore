package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.midpoint.Midpoint;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockSegment;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

final class RangeIndexEntryIterator implements SegmentIterator<IndexEntry> {

    private IndexSegment indexSegment;
    private final Direction direction;
    private final Range range;
    private final int startVersion;
    private final int endVersion;
    private final IndexEntrySerializer indexEntrySerializer;
    private final BlockSegment<IndexEntry> blockSegment;

    private long lastBlockPos;
    private int lastReadVersion;
    private Queue<IndexEntry> entries = new LinkedList<>();
    private SegmentIterator<Block> lock;

    RangeIndexEntryIterator(IndexSegment indexSegment, Direction direction, Range range, int startVersion, int endVersion, SegmentIterator<Block> lock, BlockSegment<IndexEntry> blockSegment, IndexEntrySerializer indexEntrySerializer) {
        this.indexSegment = indexSegment;
        this.direction = direction;
        this.range = range;
        this.startVersion = startVersion;
        this.endVersion = endVersion;
        this.indexEntrySerializer = indexEntrySerializer;
        this.lastReadVersion = Direction.FORWARD.equals(direction) ? startVersion - 1 : endVersion + 1;
        this.blockSegment = blockSegment;
        this.lock = lock;
    }

    private void fetchEntries() {
        int nextVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        if (nextVersion < startVersion || nextVersion > endVersion) {
            return;
        }

        IndexEntry key = IndexEntry.of(range.stream, nextVersion, 0);
        Midpoint lowBound = indexSegment.midpoints.getMidpointFor(key);
        if (lowBound == null) {//false positive on the bloom filter and entry was within range of this segment
            return;
        }
        IndexBlock foundBlock = (IndexBlock) blockSegment.getBlock(lowBound.position);
        this.lastBlockPos = lowBound.position;
        List<IndexEntry> blockEntries = foundBlock.deserialize(indexEntrySerializer);
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
        return indexSegment.readOnly() && !hasNext();
    }
}
