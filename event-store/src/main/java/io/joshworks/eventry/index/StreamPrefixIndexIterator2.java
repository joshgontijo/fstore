package io.joshworks.eventry.index;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;

import java.util.function.Function;

class StreamPrefixIndexIterator2 extends IndexIterator {

    private final String prefix;
    private final Function<String, Checkpoint> streamMatcher;

    public StreamPrefixIndexIterator2(LsmTree<IndexKey, Long> delegate, Direction direction, Checkpoint checkpoint, String prefix, Function<String, Checkpoint> streamMatcher) {
        super(delegate, direction, checkpoint);
        this.prefix = prefix;
        this.streamMatcher = streamMatcher;
    }

    @Override
    protected long nextStream() {
        if (!streamIt.hasNext()) {
            Checkpoint newItems = streamMatcher.apply(prefix);
            super.checkpoint.merge(newItems);
            streamIt = checkpoint.iterator();
        }
        if (streamIt.hasNext()) {
            return streamIt.next().getKey();
        }
        return currentStream;
    }
}