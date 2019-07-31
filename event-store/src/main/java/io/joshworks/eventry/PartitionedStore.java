package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.partition.Partition;
import io.joshworks.eventry.partition.Partitions;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;

public class PartitionedStore implements IEventStore {

    private final Partitions partitions;

    public PartitionedStore(Partitions partitions) {
        this.partitions = partitions;
    }

    public int partitionId(String stream) {
        return partitions.select(stream).id;
    }

    public int partitions() {
        return partitions.size();
    }

    public void forEachPartition(Consumer<Partition> consumer) {
        for (Partition partition : partitions.all()) {
            consumer.accept(partition);
        }
    }

    @Override
    public void compact() {
        partitions.all().stream().map(Partition::store).forEach(IEventStore::compact);
    }

    @Override
    public void close() {
        partitions.all().forEach(Partition::close);
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        //TODO stream validation might be needed here
        return partitions.select(event.stream).store().linkTo(stream, event);
    }

    @Override
    public EventRecord linkTo(String dstStream, EventId source, String sourceType) {
        return partitions.select(source.name()).store().linkTo(dstStream, source, sourceType);
    }

    @Override
    public EventRecord append(EventRecord event) {
        return partitions.select(event.stream).store().append(event);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return partitions.select(event.stream).store().append(event, expectedVersion);
    }

    @Override
    public EventStoreIterator fromStream(EventId stream) {
        return partitions.select(stream.name()).store().fromStream(stream);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap eventMap, Set<String> streamPatterns) {
        List<EventStoreIterator> iterators = partitions.all().stream()
                .map(Partition::store)
                .map(s -> s.fromStreams(eventMap, streamPatterns))
                .collect(Collectors.toList());

        return new PartitionedEventStoreIterator(iterators);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap eventMap) {
        //partition -> checkpoint per partition
        Map<Partition, EventMap> grouped = eventMap.entrySet()
                .stream()
                .map(s -> Pair.of(s, partitions.select(s.getKey())))
                .collect(groupingBy(Pair::right, mapping(kv -> EventMap.of(kv.left.getKey(), kv.left.getValue()), reducing(EventMap.empty(), EventMap::merge))));

        List<EventStoreIterator> iterators = grouped.entrySet()
                .stream()
                .map(kv -> kv.getKey().store().fromStreams(kv.getValue()))
                .collect(Collectors.toList());

        return new PartitionedEventStoreIterator(iterators);
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        throw new UnsupportedOperationException("From all is not supported in a partitioned store");
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        throw new UnsupportedOperationException("From all is not supported in a partitioned store");
    }

    public EventStoreIterator fromAll(int partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return partitions.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy);
    }

    public EventStoreIterator fromAll(int partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        return partitions.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy, lastEvent);
    }

    @Override
    public void createStream(String stream) {
        partitions.select(stream).store().createStream(stream);
    }

    @Override
    public void createStream(String stream, int maxCount, long maxAge) {
        partitions.select(stream).store().createStream(stream, maxCount, maxAge);
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        return partitions.select(stream).store().createStream(stream, maxCount, maxAge, acl, metadata);
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return partitions.all().stream()
                .map(Partition::store)
                .flatMap(es -> es.streamsMetadata().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return partitions.select(stream).store().streamMetadata(stream);
    }

    @Override
    public void truncate(String stream, int fromVersion) {
        partitions.select(stream).store().truncate(stream, fromVersion);
    }

    @Override
    public EventRecord get(EventId stream) {
        return partitions.select(stream.name()).store().get(stream);
    }

    @Override
    public int version(String stream) {
        return partitions.select(stream).store().version(stream);
    }

    @Override
    public int count(String stream) {
        return partitions.select(stream).store().count(stream);
    }


    //Round robin
    private static final class PartitionedEventStoreIterator implements EventStoreIterator {

        private final List<EventStoreIterator> iterators;
        private int partitionIdx;

        private PartitionedEventStoreIterator(List<EventStoreIterator> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            for (int i = 0; i < iterators.size(); partitionIdx++) {
                if (partitionIdx >= iterators.size()) {
                    partitionIdx = 0;
                }
                if (iterators.get(partitionIdx).hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public EventRecord next() {
            if (!hasNext()) {
                return null;
            }
            return iterators.get(partitionIdx).next();
        }

        @Override
        public void close() {
            for (EventStoreIterator iterator : iterators) {
                IOUtils.closeQuietly(iterator);
            }
        }

        @Override
        public EventMap checkpoint() {
            return iterators.stream()
                    .map(EventStoreIterator::checkpoint)
                    .reduce(EventMap.empty(), EventMap::merge);
        }
    }
}
