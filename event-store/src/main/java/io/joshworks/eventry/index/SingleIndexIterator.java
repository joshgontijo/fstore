package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.log.Direction;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SingleIndexIterator implements IndexIterator {

    private final IndexAppender diskIndex;
    private final Function<Direction, Iterator<MemIndex>> memIndex;
    private final Direction direction;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Queue<IndexEntry> buffer;
    private final long stream;
    private int lastReadVersion;

    SingleIndexIterator(IndexAppender diskIndex, Function<Direction, Iterator<MemIndex>> memIndex, Direction direction, long stream, int lastReadVersion) {
        this(diskIndex, memIndex, direction, stream, lastReadVersion, -1);
    }

    SingleIndexIterator(IndexAppender diskIndex, Function<Direction, Iterator<MemIndex>> memIndex, Direction direction,  long stream, int lastReadVersion, int bufferSize) {
        this.diskIndex = diskIndex;
        this.memIndex = memIndex;
        this.stream = stream;
        this.lastReadVersion = lastReadVersion;
        this.direction = direction;
        this.buffer = bufferSize <= 0 ? new ArrayDeque<>() : new ArrayBlockingQueue<>(bufferSize);
    }

    private IndexEntry checkConsistency(IndexEntry ie) {
        if (ie == null) {
            return null;
        }
        if (Direction.FORWARD.equals(direction) && lastReadVersion >= ie.version) {
            throw new IllegalStateException("Reading already processed version, last processed version: " + lastReadVersion + " read version: " + ie.version);
        }
        if (Direction.BACKWARD.equals(direction) && lastReadVersion <= ie.version) {
            throw new IllegalStateException("Reading already processed version, last processed version: " + lastReadVersion + " read version: " + ie.version);
        }
        int expected = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        if (expected != ie.version) {
            throw new IllegalStateException("Next expected version: " + expected + " got: " + ie.version + ", stream " + ie.stream);
        }
        lastReadVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        return ie;
    }

    private boolean fetchEntries() {
        if (!buffer.isEmpty()) {
            return true;
        }
        int nextVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        List<IndexEntry> fromDisk = diskIndex.getBlockEntries(stream, nextVersion);
        List<IndexEntry> filtered = filter(fromDisk);
        if (!filtered.isEmpty()) {
            addToBuffer(filtered);
            return true;
        }
        Iterator<MemIndex> writeQueueIt = memIndex.apply(direction);
        while (writeQueueIt.hasNext()) {
            MemIndex index = writeQueueIt.next();
            List<IndexEntry> memFiltered = fromMem(index, stream, nextVersion);
            if (!memFiltered.isEmpty()) {
                addToBuffer(memFiltered);
                return true;
            }
        }
        return false;
    }

    private void addToBuffer(List<IndexEntry> entries) {
        for (IndexEntry entry : entries) {
            if(!buffer.offer(entry)) {
                return;
            }
        }
    }

    private List<IndexEntry> fromMem(MemIndex index, long stream, int nextVersion) {
        List<IndexEntry> fromMemory = index.indexedIterator(Direction.FORWARD, Range.of(stream, nextVersion)).stream().collect(Collectors.toList());
        return filter(fromMemory);
    }

    private List<IndexEntry> filter(List<IndexEntry> original) {
        return original.stream().filter(ie -> {
            if (ie.stream != stream) {
                return false;
            }
            if (Direction.FORWARD.equals(direction)) {
                return ie.version > lastReadVersion;
            }
            return ie.version < lastReadVersion;
        }).collect(Collectors.toList());
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty() || fetchEntries();
    }

    @Override
    public IndexEntry next() {
        if (!hasNext()) {
            return null;
        }
        return checkConsistency(buffer.poll());
    }

    @Override
    public long position() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public Checkpoint processed() {
        return Checkpoint.of(stream, lastReadVersion);
    }

    @Override
    public String toString() {
        return "IndexIterator{" + "direction=" + direction +
                ", bufferSize=" + buffer.size() +
                ", stream=" + stream +
                ", lastReadVersion=" + lastReadVersion +
                '}';
    }
}
