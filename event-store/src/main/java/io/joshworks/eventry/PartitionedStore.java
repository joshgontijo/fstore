package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PartitionedStore implements IEventStore {


    private final List<IEventStore> stores;

    public PartitionedStore(File root, int partitions) {
        this.stores = stores;
    }

    public int select(String stream, int buckets) {
        return select(EventId.hash(stream), buckets);
    }

    public int select(long streamHash, int buckets) {
        return (int) (Math.abs(streamHash) % buckets);
    }

    @Override
    public void compact() {
        for (IEventStore store : stores) {
            store.compact();
        }
    }

    @Override
    public void close() {
        for (IEventStore store : stores) {
            store.close();
        }
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return null;
    }

    @Override
    public EventRecord linkTo(String dstStream, EventId source, String sourceType) {
        return null;
    }

    @Override
    public EventRecord append(EventRecord event) {
        return null;
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return null;
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return null;
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        return null;
    }

    @Override
    public void createStream(String stream) {

    }

    @Override
    public void createStream(String stream, int maxCount, long maxAge) {

    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        return null;
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return null;
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return Optional.empty();
    }

    @Override
    public void truncate(String stream, int fromVersion) {

    }

    @Override
    public EventStoreIterator fromStream(EventId stream) {
        return null;
    }

    @Override
    public EventStoreIterator fromStreams(EventMap checkpoint, Set<String> streamPatterns) {
        return null;
    }

    @Override
    public EventStoreIterator fromStreams(EventMap streams) {
        return null;
    }

    @Override
    public EventRecord get(EventId stream) {
        return null;
    }

    @Override
    public int version(String stream) {
        return 0;
    }

    @Override
    public int count(String stream) {
        return 0;
    }
}
