package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.IStream;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.partition.Partition;
import io.joshworks.eventry.server.cluster.partition.Partitions;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PartitionedStore implements IEventStore {

    private final Partitions partitions;

    PartitionedStore(Partitions partitions) {
        this.partitions = partitions;
    }

    private IEventStore select(String stream) {
        return partitions.select(stream).store();
    }

    public IEventStore partition(int partition) {
        return partitions.get(partition).store();
    }

    @Override
    public void compact() {
        partitions.all().stream().map(Partition::store).forEach(IEventStore::compact);
    }

    @Override
    public void close() {
        partitions.close();
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return select(event.stream).linkTo(stream, event);
    }

    @Override
    public EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
        return select(source.name()).linkTo(dstStream, source, sourceType);
    }

    @Override
    public EventRecord append(EventRecord event) {
        return select(event.stream).append(event);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return select(event.stream).append(event, expectedVersion);
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        return select(stream.name()).fromStream(stream);
    }


    //TODO this also needs another methods that accepts a Checkpoint
    @Override
    public EventLogIterator fromStreams(String streamPattern) {
        return applyToAll(store -> store.fromStreams(streamPattern));
    }

    //TODO this is not ideal, since it will return iterator of all partitions
    //the infinite iterators make more difficult to handle this this, since the stream can be created anywhere
    @Override
    public EventLogIterator fromStreams(Set<StreamName> streams) {
        return applyToAll(store -> store.fromStreams(streams));
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return applyToAll(store -> store.fromAll(linkToPolicy, systemEventPolicy));
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        //TODO each partition must keep track of the last read item
        //event-store should have its own iterator, that instead returning the position, returns the StreamName of last read
        //Last read event should be a Checkpoint type instead, that can hold multiple StreamName
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void createStream(String name) {
        select(name).createStream(name);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        select(name).createStream(name, maxCount, maxAge);
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        return select(stream).createStream(stream, maxCount, maxAge, acl, metadata);
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return partitions.all().stream()
                .map(Partition::store)
                .map(IStream::streamsMetadata)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return select(stream).streamMetadata(stream);
    }

    @Override
    public void truncate(String stream, int fromVersion) {
        select(stream).truncate(stream, fromVersion);
    }

    @Override
    public EventRecord get(StreamName stream) {
        return select(stream.name()).get(stream);
    }

    @Override
    public int version(String stream) {
        return select(stream).version(stream);
    }

    @Override
    public int count(String stream) {
        return select(stream).count(stream);
    }

    private EventLogIterator applyToAll(Function<IEventStore, EventLogIterator> func) {
        List<EventLogIterator> iterators = partitions.all()
                .stream()
                .map(Partition::store)
                .map(func)
                .collect(Collectors.toList());

        return EventLogIterator.of(Iterators.concat(iterators));
    }

}
