package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FixedIndexIterator implements IndexIterator {

    private final LsmTree<IndexKey, Long> delegate;
    protected final Checkpoint checkpoint;
    private final Direction direction;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long currentStream;
    private Iterator<Map.Entry<Long, Integer>> streamIt;
    private IndexEntry next;

    FixedIndexIterator(LsmTree<IndexKey, Long> delegate, Direction direction, Checkpoint checkpoint) {
        this.delegate = delegate;
        this.checkpoint = checkpoint;
        this.direction = direction;
        this.streamIt = checkpoint.iterator();
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

    private IndexEntry fetch() {
        if (next != null || checkpoint.size() == 0) {
            return next;
        }

        currentStream = nextStream();
        long startStream = currentStream;
        do {
            int lastReadVersion = checkpoint.get(currentStream);
            next = fetchEntry(currentStream, lastReadVersion);
            if (next == null) {
                currentStream = nextStream();
            }
        } while (startStream != currentStream && next == null);

        return next;
    }

    private IndexEntry fetchEntry(long stream, int lastReadVersion) {
        int version = lastReadVersion + 1;
        Entry<IndexKey, Long> entry = delegate.getEntry(IndexKey.event(stream, version));
        if (entry == null) {
            return null;
        }
        IndexEntry indexEntry = IndexEntry.of(stream, version, entry.value, entry.timestamp);
        return isReadable(indexEntry) ? indexEntry : null;
    }

    protected synchronized long nextStream() {
        if (checkpoint.size() == 0) {
            return 0;
        }
        if (!streamIt.hasNext()) {
            streamIt = checkpoint.iterator();
        }
        return streamIt.next().getKey();
    }

    private boolean isReadable(IndexEntry ie) {
        return !ie.isDeletion() && !ie.isTruncation();
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        next = fetch();
        return next != null;
    }

    @Override
    public IndexEntry next() {
        if (!hasNext()) {
            return null;
        }
        IndexEntry tmp = next;
        next = null;
        return checkConsistency(tmp);
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public Checkpoint checkpoint() {
        return checkpoint;
    }

    //Stream listeners
    @Override
    public void onStreamCreated(StreamMetadata metadata) {

    }

    @Override
    public void onStreamTruncated(StreamMetadata metadata) {
        checkpoint.put(metadata.hash, metadata.truncated);
    }

    @Override
    public void onStreamDeleted(StreamMetadata metadata) {

    }
}
