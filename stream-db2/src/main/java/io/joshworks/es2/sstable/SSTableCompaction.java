package io.joshworks.es2.sstable;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.CompactionItem;
import io.joshworks.fstore.core.iterators.CloseableIterator;
import io.joshworks.fstore.core.iterators.Iterators;
import io.joshworks.fstore.core.iterators.PeekingIterator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.min;

class SSTableCompaction implements Compaction<SSTable> {

    private final SSTableConfig config;

    SSTableCompaction(SSTableConfig config) {
        this.config = config;
    }

    @Override
    public void compact(CompactionItem<SSTable> handle) {

        long expectedEntries = handle.sources().stream().mapToLong(SSTable::denseEntries).sum();
        long totalSize = handle.sources().stream().mapToLong(SSTable::size).sum();

        expectedEntries = min(expectedEntries, Integer.MAX_VALUE); //bloom filter will return more false positives for this segment

        var iterators = handle.sources()
                .stream()
                .map(s -> s.channel)
                .map(LengthPrefixedChannelIterator::new)
                .map(StreamBlockIterator::new)
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());

        var merging = Iterators.merging(iterators, StreamBlock::compare);
        var levelConfig = handle.nextLevel() >= this.config.levelThreshold ? this.config.highConfig : this.config.lowConfig;
        SSTable.create(handle.replacement(), merging, (int) expectedEntries, totalSize, levelConfig)
                .close();
    }
}

