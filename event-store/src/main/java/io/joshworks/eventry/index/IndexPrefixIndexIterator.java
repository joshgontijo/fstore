package io.joshworks.eventry.index;

import io.joshworks.eventry.EventMap;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.LsmTree;

import java.util.Set;

import static io.joshworks.eventry.EventId.NO_VERSION;

class IndexPrefixIndexIterator extends FixedIndexIterator {

    private final Set<String> patterns;

    IndexPrefixIndexIterator(LsmTree<IndexKey, Long> delegate, Direction direction, EventMap eventMap, Set<String> streamPatterns) {
        super(delegate, direction, eventMap);
        this.patterns = streamPatterns;
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {
        if (Streams.matchAny(metadata.name, patterns)) {
            eventMap.add(metadata.hash, NO_VERSION);
        }
    }
}