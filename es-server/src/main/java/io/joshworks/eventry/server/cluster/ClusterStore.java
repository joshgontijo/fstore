package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ClusterStore implements IEventStore {

    private final Partitions partitions;

    public ClusterStore(Partitions partitions) {
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
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void close() {
        partitions.close();
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return null;
    }

    @Override
    public EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
        return null;
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

    @Override
    public EventLogIterator fromStreams(String streamPattern, boolean ordered) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streams, boolean ordered) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
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
        throw new UnsupportedOperationException("TODO");
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
}
