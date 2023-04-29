package io.joshworks.fstore.index;

import io.joshworks.fstore.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class FixedIndexIterator implements IndexIterator {

    protected final EventMap eventMap;
    private final SSTables<IndexKey, Long> delegate;
    private final Direction direction;
    private final AtomicBoolean closed = new AtomicBoolean();
    private long currentStream;
    private Iterator<Map.Entry<Long, Integer>> streamIt;
    private IndexEntry next;

    FixedIndexIterator(SSTables<IndexKey, Long> delegate, Direction direction, EventMap eventMap) {
        this.delegate = delegate;
        this.eventMap = eventMap;
        this.direction = direction;
        this.streamIt = eventMap.iterator();
    }

    private IndexEntry checkConsistency(IndexEntry ie) {
        if (ie == null) {
            return null;
        }
        int lastReadVersion = eventMap.get(currentStream);
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
        //updates with the read version
        eventMap.add(currentStream, nextVersion);
        return ie;
    }

    private IndexEntry fetch() {
        if (next != null || eventMap.size() == 0) {
            return next;
        }

        currentStream = nextStream();
        long startStream = currentStream;
        do {
            int lastReadVersion = eventMap.get(currentStream);
            next = fetchEntry(currentStream, lastReadVersion);
            if (next == null) {
                currentStream = nextStream();
            }
        } while (startStream != currentStream && next == null);

        return next;
    }

    private IndexEntry fetchEntry(long stream, int lastReadVersion) {
        int version = Direction.FORWARD.equals(direction) ? lastReadVersion + 1 : lastReadVersion - 1;
        Entry<IndexKey, Long> entry = delegate.get(IndexKey.event(stream, version));
        if (entry == null) {
            return null;
        }
        IndexEntry indexEntry = IndexEntry.of(stream, version, entry.value, entry.timestamp);
        return isReadable(indexEntry) ? indexEntry : null;
    }

    private synchronized long nextStream() {
        if (eventMap.size() == 0) {
            return 0;
        }
        if (!streamIt.hasNext()) {
            streamIt = eventMap.iterator();
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
    public EventMap checkpoint() {
        return eventMap;
    }

    //Stream listeners
    @Override
    public void onStreamCreated(StreamMetadata metadata) {

    }

    @Override
    public void onStreamTruncated(StreamMetadata metadata) {
        eventMap.add(metadata.hash, metadata.truncated);
    }

    @Override
    public void onStreamDeleted(StreamMetadata metadata) {

    }
}
