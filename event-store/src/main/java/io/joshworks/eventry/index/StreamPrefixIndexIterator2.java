package io.joshworks.eventry.index;

import io.joshworks.eventry.index.disk.IndexAppender;
import io.joshworks.fstore.log.Direction;

import java.util.Iterator;
import java.util.function.Function;

class StreamPrefixIndexIterator2 extends SingleIndexIterator {

    private final String prefix;
    private final Function<String, Checkpoint> streamMatcher;

    public StreamPrefixIndexIterator2(IndexAppender diskIndex, Function<Direction, Iterator<MemIndex>> memIndex, Direction direction, Checkpoint checkpoint, String prefix, Function<String, Checkpoint> streamMatcher) {
        super(diskIndex, memIndex, direction, checkpoint);
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