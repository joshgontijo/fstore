package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;

import static io.joshworks.eventry.StreamName.NO_VERSION;

class IndexPrefixIndexIterator extends FixedIndexIterator {

    private final String[] patterns;

    IndexPrefixIndexIterator(LsmTree<IndexKey, Long> delegate, Direction direction, Checkpoint checkpoint, String... streamPatterns) {
        super(delegate, direction, checkpoint);
        this.patterns = streamPatterns;
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {
        if (Streams.matchAny(metadata.name, patterns)) {
            checkpoint.put(metadata.hash, NO_VERSION);
        }
    }
}