package io.joshworks;

import io.joshworks.fstore.log.LogIterator;

public class ReplicatedIterator implements LogIterator<Record> {

    private final String nodeId;
    private final CommitTable commitTable;
    private final LogIterator<Record> delegate;
    private long currentSequence;

    public ReplicatedIterator(String nodeId, CommitTable commitTable, LogIterator<Record> delegate) {
        this.nodeId = nodeId;
        this.commitTable = commitTable;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return currentSequence <= commitTable.get(nodeId) && delegate.hasNext();
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            return null;
        }
        Record next = delegate.next();
        currentSequence = next.sequence;
        return next;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        delegate.close();
    }

}
