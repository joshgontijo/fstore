package io.joshworks.eventry.index;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

class StreamPrefixIndexIterator implements IndexIterator {

    private final Map<Long, IndexIterator> iteratorMap = new HashMap<>();
    private final String prefix;
    private final Function<String, Map<Long, IndexIterator>> streamProvider;
    private final boolean ordered;
    private LogIterator<IndexEntry> delegate;

    public StreamPrefixIndexIterator(String prefix, Map<Long, IndexIterator> initialIterators, Function<String, Map<Long, IndexIterator>> streamProvider, boolean ordered) {
        this.prefix = prefix;
        this.streamProvider = streamProvider;
        this.iteratorMap.putAll(initialIterators);
        this.ordered = ordered;
        this.delegate = join(initialIterators.values());
    }

    private synchronized LogIterator<IndexEntry> newIterator() {
        Map<Long, IndexIterator> newIterators = streamProvider.apply(prefix);
        for (Map.Entry<Long, IndexIterator> entries : newIterators.entrySet()) {
            iteratorMap.putIfAbsent(entries.getKey(), entries.getValue());
        }

        return join(iteratorMap.values());
    }

    private LogIterator<IndexEntry> join(Collection<IndexIterator> iterators) {
        return ordered ? Iterators.ordered(iterators, ie -> ie.position) : Iterators.concat(iterators);
    }

    @Override
    public boolean hasNext() {
        if (!delegate.hasNext()) {
            this.delegate = newIterator();
        }
        return delegate.hasNext();
    }

    @Override
    public IndexEntry next() {
        if (!delegate.hasNext()) {
            delegate = newIterator();
            if (!delegate.hasNext()) {
                return null;
            }
        }
        return delegate.next();
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(delegate);
    }

}