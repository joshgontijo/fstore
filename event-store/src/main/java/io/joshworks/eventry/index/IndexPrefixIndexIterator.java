package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.streams.StreamPattern;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.util.Set;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;

class IndexPrefixIndexIterator extends FixedIndexIterator {

    private final Set<String> patterns;

    IndexPrefixIndexIterator(SSTables<IndexKey, Long> delegate, Direction direction, EventMap eventMap, Set<String> streamPatterns) {
        super(delegate, direction, eventMap);
        this.patterns = streamPatterns;
    }

    @Override
    public void onStreamCreated(StreamMetadata metadata) {
        if (StreamPattern.matchesPattern(metadata.name, patterns)) {
            eventMap.add(metadata.hash, NO_VERSION);
        }
    }
}