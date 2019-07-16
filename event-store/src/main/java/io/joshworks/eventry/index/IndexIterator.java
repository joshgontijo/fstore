package io.joshworks.eventry.index;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexIterator implements CloseableIterator<IndexEntry> {

    private final LsmTree<IndexKey, Long> delegate;
    protected final Checkpoint checkpoint;
    private final Direction direction;
    private final AtomicBoolean closed = new AtomicBoolean();
    protected long currentStream;
    protected Iterator<Map.Entry<Long, Integer>> streamIt;
    private IndexEntry next;


    IndexIterator(LsmTree<IndexKey, Long> delegate, Direction direction, Checkpoint checkpoint) {
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
        Long position = delegate.get(new IndexKey(stream, version));
        if (position != null) {
            IndexEntry indexEntry = IndexEntry.of(stream, version, position);
            if (isReadable(indexEntry)) {
                return indexEntry;
            }
        }
        return null;
    }

    protected long nextStream() {
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

    public void onStreamChanged() {

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

}
