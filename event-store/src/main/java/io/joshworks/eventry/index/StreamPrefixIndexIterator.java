package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;

class StreamPrefixIndexIterator extends FixedStreamIterator {

    private final String prefix;

    StreamPrefixIndexIterator(LsmTree<IndexKey, Long> delegate, Direction direction, Checkpoint checkpoint, String prefix) {
        super(delegate, direction, checkpoint);
        this.prefix = prefix;
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {
        if (Streams.matches(metadata.name, prefix)) {
            checkpoint.put(metadata.hash, NO_VERSION);
        }
    }
}