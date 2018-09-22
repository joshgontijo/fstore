package io.joshworks.eventry;

import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.ProjectionCreated;
import io.joshworks.eventry.data.ProjectionDeleted;
import io.joshworks.eventry.data.ProjectionUpdated;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.StreamDeleted;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventLog;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.log.RecordCleanup;
import io.joshworks.eventry.projections.ExecutionStatus;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.Projections;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.eventry.utils.Tuple;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventStore implements Closeable {

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;

    private final TableIndex index;
    private final Streams streams;
    private final EventLog eventLog;
    private final Projections projections;

    private EventStore(File rootDir) {
        this.index = new TableIndex(rootDir);
        this.projections = new Projections();
        this.streams = new Streams(LRU_CACHE_SIZE, index::version);
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer())
                .segmentSize((int) Size.MEGABYTE.toBytes(200))
                .disableCompaction()
                .compactionStrategy(new RecordCleanup(streams)));

        this.loadIndex();
        this.loadStreams();
        this.loadProjections();
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }


    private void loadIndex() {
        try (LogIterator<EventRecord> iterator = eventLog.iterator(Direction.BACKWARD)) {

            while (iterator.hasNext()) {
                EventRecord next = iterator.next();
                long position = iterator.position();
                if (next.isSystemEvent() && IndexFlushed.TYPE.equals(next.type)) {
                    break;
                }
                long streamHash = streams.hashOf(next.stream);
                index.add(streamHash, next.version, position);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load memIndex", e);
        }

    }

    private void loadStreams() {

        long streamHash = streams.hashOf(SystemStreams.STREAMS);
        LogIterator<IndexEntry> addresses = index.iterator(Direction.FORWARD, Range.allOf(streamHash));

        while (addresses.hasNext()) {
            IndexEntry next = addresses.next();
            EventRecord event = eventLog.get(next.position);

            //pattern matching would be great here
            if (StreamCreated.TYPE.equals(event.type)) {
                streams.add(StreamCreated.from(event));
            } else if (StreamDeleted.TYPE.equals(event.type)) {
                StreamDeleted deleted = StreamDeleted.from(event);
                long hash = streams.hashOf(deleted.stream);
                streams.remove(hash);
            } else {
                //unrecognized event
            }
        }
    }

    private void loadProjections() {
        long streamHash = streams.hashOf(SystemStreams.PROJECTIONS);
        LogIterator<IndexEntry> addresses = index.iterator(Direction.FORWARD, Range.allOf(streamHash));

        while (addresses.hasNext()) {
            IndexEntry next = addresses.next();
            EventRecord event = eventLog.get(next.position);

            //pattern matching would be great here
            if (ProjectionCreated.TYPE.equals(event.type)) {
                projections.add(ProjectionCreated.from(event));
            } else if (ProjectionDeleted.TYPE.equals(event.type)) {
                ProjectionDeleted deleted = ProjectionDeleted.from(event);
                projections.delete(deleted.name);
            }
        }
    }

    public LogIterator<IndexEntry> keys() {
        return index.iterator(Direction.FORWARD);
    }

    public void cleanup() {
        eventLog.cleanup();
    }

    public void createStream(String name) {
        createStream(name, -1, -1);
    }

    public Collection<Projection> projections() {
        return new ArrayList<>(projections.all());
    }

    public Projection projection(String name) {
        return projections.get(name);
    }

    public Projection createProjection(String name, String script, Projection.Type type, boolean enabled) {
        Projection projection = projections.create(name, script, type, enabled);
        EventRecord eventRecord = ProjectionCreated.create(projection);
        this.appendSystemEvent(eventRecord);

        return projection;
    }

    public Projection updateProjection(String name, String script, Projection.Type type, Boolean enabled) {
        Projection projection = projections.update(name, script, type, enabled);
        EventRecord eventRecord = ProjectionUpdated.create(projection);
        this.appendSystemEvent(eventRecord);

        return projection;
    }

    public void deleteProjection(String name) {
        projections.delete(name);
        EventRecord eventRecord = ProjectionDeleted.create(name);
        this.appendSystemEvent(eventRecord);
    }

    public void runProjection(String name) {
        projections.run(name, this, this::appendSystemEvent);
    }

    public ExecutionStatus projectionExecutionStatus(String name) {
        return projections.executionStatus(name);
    }

    public Collection<ExecutionStatus> projectionExecutionStatuses() {
        return projections.executionStatuses();
    }

    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, maxCount, maxAge, new HashMap<>(), new HashMap<>());
    }

    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        StreamMetadata created = streams.create(stream, maxAge, maxCount, permissions, metadata);
        if (created == null) {
            throw new IllegalStateException("Stream '" + stream + "' already exist");
        }
        EventRecord eventRecord = StreamCreated.create(created);
        this.appendSystemEvent(eventRecord);
        return created;
    }

    public List<StreamInfo> streamsMetadata() {
        return streams.all().stream().map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        }).collect(Collectors.toList());
    }

    public Optional<StreamInfo> streamMetadata(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.get(streamHash).map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        });
    }

    public LogIterator<EventRecord> fromStreamIter(String stream) {
        return fromStreamIter(stream, Range.START_VERSION);
    }

    public Stream<EventRecord> fromStream(String stream) {
        return fromStream(stream, Range.START_VERSION);
    }

    public LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive) {
        long streamHash = streams.hashOf(stream);
        LogIterator<IndexEntry> indexIterator = index.iterator(Direction.FORWARD, Range.of(streamHash, versionInclusive));
        indexIterator = withMaxCountFilter(streamHash, indexIterator);
        SingleStreamIterator singleStreamIterator = new SingleStreamIterator(indexIterator, eventLog);
        LogIterator<EventRecord> ageFilterIterator = withMaxAgeFilter(Set.of(streamHash), singleStreamIterator);
        return new LinkToResolveIterator(ageFilterIterator, this::resolve);

    }

    public Stream<EventRecord> fromStream(String stream, int versionInclusive) {
        LogIterator<EventRecord> iterator = fromStreamIter(stream, versionInclusive);
        return Iterators.stream(iterator);
    }

    public Stream<EventRecord> zipStreams(Set<String> streams) {
        LogIterator<EventRecord> iterator = zipStreamsIter(streams);
        return Iterators.stream(iterator);
    }

    public LogIterator<EventRecord> zipStreamsIter(String stream) {
        Set<String> eventStreams = streams.streamMatching(stream);
        if (eventStreams.isEmpty()) {
            return Iterators.empty();
        }
        return zipStreamsIter(eventStreams);
    }

    public Stream<EventRecord> zipStreams(String streamPrefix) {
        return Iterators.stream(zipStreamsIter(streamPrefix));
    }

    public LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames) {

        Set<Long> hashes = streamNames.stream()
                .filter(StringUtils::nonBlank)
                .map(streams::hashOf)
                .collect(Collectors.toSet());

        List<LogIterator<IndexEntry>> indexes = hashes.stream()
                .map(hash -> index.iterator(Direction.FORWARD, Range.allOf(hash)))
                .collect(Collectors.toList());

        LogIterator<EventRecord> ageFilterIterator = withMaxAgeFilter(hashes, new MultiStreamIterator(indexes, eventLog));
        return new LinkToResolveIterator(ageFilterIterator, this::resolve);
    }

    public Stream<Stream<EventRecord>> fromStreams(Set<String> streams) {
        return streams.stream().map(this::fromStream);
    }

    public Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams) {
        return streams.stream()
                .map(stream -> Tuple.of(stream, fromStream(stream)))
                .collect(Collectors.toMap(Tuple::a, Tuple::b));
    }

    public int version(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.version(streamHash);
    }

    public LogIterator<EventRecord> fromAllIter() {
        return eventLog.iterator(Direction.FORWARD);
    }

    //Won't return the stream in the event !
    public Stream<EventRecord> fromAll() {
        return eventLog.stream(Direction.FORWARD);
    }

    public EventRecord linkTo(String stream, EventRecord event) {
        if (event.isLinkToEvent()) {
            //resolve event
            event = get(event.stream, event.version);
        }
        EventRecord linkTo = LinkTo.create(stream, event);
        return this.appendSystemEvent(linkTo);
    }

    public void emit(String stream, EventRecord event) {
        EventRecord withStream = EventRecord.create(stream, event.type, event.data, event.metadata);
        this.append(withStream);
    }

    public EventRecord get(String stream, int version) {
        long streamHash = streams.hashOf(stream);

        if (version <= IndexEntry.NO_VERSION) {
            throw new IllegalArgumentException("Version must be greater than " + IndexEntry.NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(streamHash, version);
        if (!indexEntry.isPresent()) {
            //TODO improve this to a non exception response
            throw new RuntimeException("IndexEntry not found for " + stream + "@" + version);
        }

        return indexEntry.map(this::get).orElseThrow(() -> new RuntimeException("EventRecord not found for " + indexEntry));
    }


    //TODO make it price and change SingleStreamIterator
    EventRecord get(IndexEntry indexEntry) {
        Objects.requireNonNull(indexEntry, "IndexEntry must be provided");
        EventRecord record = eventLog.get(indexEntry.position);

        return resolve(record);
    }

    EventRecord resolve(EventRecord record) {
        if (record.isLinkToEvent()) {
            String[] split = record.dataAsString().split(EventRecord.STREAM_VERSION_SEPARATOR);
            var linkToStream = split[0];
            var linkToVersion = Integer.parseInt(split[1]);
            return get(linkToStream, linkToVersion);
        }
        return record;
    }

    private void validateEvent(EventRecord event) {
        Objects.requireNonNull(event, "Event must be provided");
        StringUtils.requireNonBlank(event.stream, "stream must be provided");
        StringUtils.requireNonBlank(event.type, "Type must be provided");
        if (event.stream.startsWith(Constant.SYSTEM_PREFIX)) {
            throw new IllegalArgumentException("Stream cannot start with " + Constant.SYSTEM_PREFIX);
        }
    }

    public EventRecord append(EventRecord event) {
        return append(event, IndexEntry.NO_VERSION);
    }

    public EventRecord append(EventRecord event, int expectedVersion) {
        validateEvent(event);

        StreamMetadata metadata = getOrCreateStream(event.stream);
        return append(metadata, event, expectedVersion);
    }

    private synchronized EventRecord appendSystemEvent(EventRecord event) {
        StreamMetadata metadata = getOrCreateStream(event.stream);
        return append(metadata, event, IndexEntry.NO_VERSION);
    }

    private EventRecord append(StreamMetadata streamMetadata, EventRecord event, int expectedVersion) {
        if (streamMetadata == null) {
            throw new IllegalArgumentException("EventStream cannot be null");
        }
        long streamHash = streams.hashOf(event.stream);
        if (streamMetadata.name.equals(event.stream) && streamMetadata.hash != streamHash) {
            //TODO improve ??
            throw new IllegalStateException("Hash collision of stream: " + event.stream + " with existing name: " + streamMetadata.name);
        }

        int version = streams.tryIncrementVersion(streamHash, expectedVersion);

        var record = new EventRecord(event.stream, event.type, version, System.currentTimeMillis(), event.data, event.metadata);

        long position = eventLog.append(record);
        var flushInfo = index.add(streamHash, version, position);
        if (flushInfo != null) {
            var indexFlushedEvent = IndexFlushed.create(position, flushInfo.timeTaken, flushInfo.entries);
            this.appendSystemEvent(indexFlushedEvent);
        }

        return record;
    }

    private StreamMetadata getOrCreateStream(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.get(streamHash).orElseGet(() -> {
            StreamMetadata created = streams.create(stream);
            if (created != null) { // metadata was created
                EventRecord eventRecord = StreamCreated.create(created);
                this.appendSystemEvent(eventRecord);
            }
            return created;
        });
    }

    public PollingSubscriber<EventRecord> poller() {
        return new LogPoller(eventLog.poller(), this);
    }

    public PollingSubscriber<EventRecord> poller(long position) {
        return new LogPoller(eventLog.poller(position), this);
    }

    public PollingSubscriber<EventRecord> poller(String stream) {
        Set<Long> hashes = streams.streamMatching(stream).stream().map(streams::hashOf).collect(Collectors.toSet());
        return new IndexedLogPoller(index.poller(hashes), this);
    }

    public PollingSubscriber<EventRecord> poller(Set<String> streamNames) {
        Set<Long> hashes = streamNames.stream().map(streams::hashOf).collect(Collectors.toSet());
        return new IndexedLogPoller(index.poller(hashes), this);
    }

//    public PollingSubscriber<Event> poller(String stream, int version) {
//        long streamHash = streams.hashOf(stream);
//        return new IndexedLogPoller(index.poller(streamHash, version), eventLog);
//    }

    private LogIterator<IndexEntry> withMaxCountFilter(long streamHash, LogIterator<IndexEntry> iterator) {
        return streams.get(streamHash)
                .map(stream -> stream.maxCount)
                .filter(maxCount -> maxCount > 0)
                .map(maxCount -> MaxCountFilteringIterator.of(maxCount, streams.version(streamHash), iterator))
                .orElse(iterator);
    }

    private LogIterator<EventRecord> withMaxAgeFilter(Set<Long> streamHashes, LogIterator<EventRecord> iterator) {
        Map<String, Long> metadataMap = streamHashes.stream()
                .map(streams::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(StreamMetadata::name, meta -> meta.maxAge));

        return new MaxAgeFilteringIterator(metadataMap, iterator);
    }

    public long logPosition() {
        return eventLog.position();
    }

    @Override
    public void close() {
        index.close();
        eventLog.close();
        streams.close();
    }

}