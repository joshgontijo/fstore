package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

class SingleIndexIterator implements IndexIterator {

    private final IndexAppender diskIndex;
    private final Function<Direction, Iterator<MemIndex>> memIndex;
    protected final Checkpoint checkpoint;
    private final Direction direction;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Queue<IndexEntry> buffer = new ArrayBlockingQueue<>(100);
    protected long currentStream;
    protected Iterator<Map.Entry<Long, Integer>> streamIt;


    SingleIndexIterator(IndexAppender diskIndex, Function<Direction, Iterator<MemIndex>> memIndex, Direction direction, Checkpoint checkpoint) {
        this.diskIndex = diskIndex;
        this.memIndex = memIndex;
        this.checkpoint = checkpoint;
        this.direction = direction;
        this.streamIt = checkpoint.iterator();
        this.currentStream = nextStream();
    }

    private IndexEntry checkConsistency(IndexEntry ie) {
        if (ie == null) {
            return null;
        }
        int lastReadVersion = checkpoint.get(currentStream);
        if (Direction.FORWARD.equals(direction) && lastReadVersion >= ie.version) {
            throw new IllegalStateException("Reading already processed version, last processed version: " + lastReadVersion + " read version: " + ie.version);
        }
        if (Direction.BACKWARD.equals(direction) && lastReadVersion <= ie.version) {
            throw new IllegalStateException("Reading already processed version, last processed version: " + lastReadVersion + " read version: " + ie.version);
        }
        int nextVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        if (ie.version != nextVersion) {
            throw new IllegalStateException("Next expected version: " + nextVersion + " got: " + ie.version + ", stream " + ie.stream);
        }
        checkpoint.update(currentStream, nextVersion);
        return ie;
    }

    private boolean fetchEntries() {
        if (!buffer.isEmpty()) {
            return true;
        }

        currentStream = nextStream();
        long startStream = currentStream;
        do {
            int lastReadVersion = checkpoint.get(currentStream);
            fetchStream(currentStream, lastReadVersion);
        } while (startStream != currentStream && buffer.isEmpty());

        return !buffer.isEmpty();
    }

    private void fetchStream(long stream, int lastReadVersion) {
        int nextVersion = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;

        Predicate<IndexEntry> filter = ie -> this.filter(ie, lastReadVersion);

        LogIterator<IndexEntry> fromDisk = Iterators.of(diskIndex.getBlockEntries(stream, nextVersion));
        LogIterator<IndexEntry> filtered = Iterators.filtering(fromDisk, filter);
        if (filtered.hasNext()) {
            addToBuffer(filtered);
            return;
        }
        Iterator<MemIndex> writeQueueIt = memIndex.apply(direction);
        while (writeQueueIt.hasNext()) {
            MemIndex index = writeQueueIt.next();
            LogIterator<IndexEntry> memFiltered = Iterators.filtering(fromMem(index, stream, nextVersion), filter);
            if (memFiltered.hasNext()) {
                addToBuffer(memFiltered);
            }
        }
    }

    protected long nextStream() {
        if (!streamIt.hasNext()) {
            streamIt = checkpoint.iterator();
        }
        return streamIt.next().getKey();
    }

    private void addToBuffer(LogIterator<IndexEntry> entries) {
        while (entries.hasNext()) {
            IndexEntry entry = entries.next();
            if (isReadable(entry)) {
                if (!buffer.offer(entry)) {
                    return;
                }
            }
        }
    }

    private boolean isReadable(IndexEntry ie) {
        return !ie.isDeletion() && !ie.isTruncation();
    }

    private LogIterator<IndexEntry> fromMem(MemIndex index, long stream, int nextVersion) {
        return index.indexedIterator(Direction.FORWARD, Range.of(stream, nextVersion));
    }

    private boolean filter(IndexEntry ie, int lastReadVersion) {
        if (ie.stream != currentStream) {
            return false;
        }
        if (Direction.FORWARD.equals(direction)) {
            return ie.version > lastReadVersion;
        }
        return ie.version < lastReadVersion;
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

}
