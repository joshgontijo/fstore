package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.Partitions;
import io.joshworks.eventry.server.cluster.data.NetworkRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.eventbus.Subscribe;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClusterStore implements EventStore {

    private final Partitions partitions;

    public ClusterStore(Partitions partitions) {
        this.partitions = partitions;
    }


    @Subscribe
    public void onEventData(NetworkRecord record) {
        EventRecord event = record.record;
        this.add(event);
    }

//    @Subscribe
//    public void onEventData(NetworkStream stream) {
//        StreamMetadata metadata = stream.metadata;
//        partitions.select(metadata.hash).createStream(metadata.name, metadata.maxCount, metadata.maxAge, new HashMap<>(), new HashMap<>());
//    }



    @Override
    public LogIterator<IndexEntry> keys() {
        List<LogIterator<IndexEntry>> allKeys = partitions.stream().map(EventStore::keys).collect(Collectors.toList());
        return Iterators.flatten(allKeys);
    }

    @Override
    public void cleanup() {
        partitions.stream().forEach(EventStore::cleanup);
    }

    @Override
    public void createStream(String name) {
        partitions.select(name).createStream(name);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        partitions.select(name).createStream(name, maxCount, maxAge);
    }

    @Override
    public StreamMetadata createStream(String name, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        return partitions.select(name).createStream(name, maxCount, maxAge, permissions, metadata);
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return partitions.stream().map(EventStore::streamsMetadata).
                flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return partitions.select(stream).streamMetadata(stream);
    }

    @Override
    public int version(String stream) {
        return partitions.select(stream).version(stream);
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return partitions.select(stream).linkTo(stream, event);
    }

    @Override
    public void emit(String stream, EventRecord event) {
        partitions.select(stream).emit(stream, event);
    }

    @Override
    public EventRecord get(String stream, int version) {
        return partitions.select(stream).get(stream, version);
    }

    @Override
    public EventRecord get(IndexEntry indexEntry) {
        return partitions.select(indexEntry.stream).get(indexEntry);
    }

    @Override
    public EventRecord append(EventRecord event) {
        return partitions.select(event.stream).append(event);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return partitions.select(event.stream).append(event, expectedVersion);
    }

    @Override
    public void add(EventRecord record) {
        partitions.select(record.stream).add(record);
    }

    //-------------- TODO implement (multi stream join from other nodes and batch reads needed) ---------------

    @Override
    public LogIterator<EventRecord> fromStreamIter(String stream) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<EventRecord> fromStream(String stream) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<EventRecord> fromStream(String stream, int versionInclusive) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<EventRecord> zipStreams(Set<String> streams) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(String stream) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<EventRecord> zipStreams(String streamPrefix) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<Stream<EventRecord>> fromStreams(Set<String> streams) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public LogIterator<EventRecord> fromAllIter() {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public Stream<EventRecord> fromAll() {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public PollingSubscriber<EventRecord> poller() {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public PollingSubscriber<EventRecord> poller(long position) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public PollingSubscriber<EventRecord> poller(String stream) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public PollingSubscriber<EventRecord> poller(Set<String> streamNames) {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public long logPosition() {
        throw new UnsupportedOperationException("Wrong node");
    }

    @Override
    public void close() {

    }

}
