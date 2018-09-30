package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.meta.Metrics;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class QueuedEventStore implements IEventStore {

    private final IEventStore delegate;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("writer");
        return t;
    });

    public QueuedEventStore(IEventStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public LogIterator<IndexEntry> keys() {
        return delegate.keys();
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return delegate.linkTo(stream, event);
    }

    @Override
    public void emit(String stream, EventRecord event) {
        delegate.emit(stream, event);
    }

    @Override
    public EventRecord append(EventRecord event) {
        try {
            Future<EventRecord> task = executor.submit(() -> delegate.append(event));
            return task.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return delegate.append(event, expectedVersion);
    }

    @Override
    public long logPosition() {
        return delegate.logPosition();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Collection<Projection> projections() {
        return delegate.projections();
    }

    @Override
    public Projection projection(String name) {
        return delegate.projection(name);
    }

    @Override
    public Projection createProjection(String name, Set<String> streams, String script, Projection.Type type, boolean enabled) {
        return delegate.createProjection(name, streams, script, type, enabled);
    }

    @Override
    public Projection updateProjection(String name, String script, Projection.Type type, Boolean enabled) {
        return delegate.updateProjection(name, script, type, enabled);
    }

    @Override
    public void deleteProjection(String name) {
        delegate.deleteProjection(name);
    }

    @Override
    public void runProjection(String name) {
        delegate.runProjection(name);
    }

    @Override
    public Metrics projectionExecutionStatus(String name) {
        return delegate.projectionExecutionStatus(name);
    }

    @Override
    public Collection<Metrics> projectionExecutionStatuses() {
        return delegate.projectionExecutionStatuses();
    }

    @Override
    public void createStream(String name) {
        delegate.createStream(name);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        delegate.createStream(name, maxCount, maxAge);
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        return delegate.createStream(stream, maxCount, maxAge, permissions, metadata);
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return delegate.streamsMetadata();
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return delegate.streamMetadata(stream);
    }

    @Override
    public EventRecord get(String stream, int version) {
        return delegate.get(stream, version);
    }

    @Override
    public LogIterator<EventRecord> fromStreamIter(String stream) {
        return delegate.fromStreamIter(stream);
    }

    @Override
    public Stream<EventRecord> fromStream(String stream) {
        return delegate.fromStream(stream);
    }

    @Override
    public LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive) {
        return delegate.fromStreamIter(stream, versionInclusive);
    }

    @Override
    public Stream<EventRecord> fromStream(String stream, int versionInclusive) {
        return delegate.fromStream(stream, versionInclusive);
    }

    @Override
    public Stream<EventRecord> zipStreams(Set<String> streams) {
        return delegate.zipStreams(streams);
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(String stream) {
        return delegate.zipStreamsIter(stream);
    }

    @Override
    public Stream<EventRecord> zipStreams(String streamPrefix) {
        return delegate.zipStreams(streamPrefix);
    }

    @Override
    public LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames) {
        return delegate.zipStreamsIter(streamNames);
    }

    @Override
    public Stream<Stream<EventRecord>> fromStreams(Set<String> streams) {
        return delegate.fromStreams(streams);
    }

    @Override
    public Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams) {
        return delegate.fromStreamsMapped(streams);
    }

    @Override
    public int version(String stream) {
        return delegate.version(stream);
    }

    @Override
    public LogIterator<EventRecord> fromAllIter() {
        return delegate.fromAllIter();
    }

    @Override
    public Stream<EventRecord> fromAll() {
        return delegate.fromAll();
    }

    @Override
    public PollingSubscriber<EventRecord> poller() {
        return delegate.poller();
    }

    @Override
    public PollingSubscriber<EventRecord> poller(long position) {
        return delegate.poller(position);
    }

    @Override
    public PollingSubscriber<EventRecord> poller(String stream) {
        return delegate.poller(stream);
    }

    @Override
    public PollingSubscriber<EventRecord> poller(Set<String> streamNames) {
        return delegate.poller(streamNames);
    }
}
