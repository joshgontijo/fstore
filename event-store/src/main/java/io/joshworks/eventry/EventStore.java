package io.joshworks.eventry;

import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.ProjectionCreated;
import io.joshworks.eventry.data.ProjectionDeleted;
import io.joshworks.eventry.data.ProjectionFailed;
import io.joshworks.eventry.data.ProjectionResumed;
import io.joshworks.eventry.data.ProjectionStarted;
import io.joshworks.eventry.data.ProjectionStopped;
import io.joshworks.eventry.data.ProjectionUpdated;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventLog;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.log.RecordCleanup;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.ProjectionExecutor;
import io.joshworks.eventry.projections.Projections;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static java.util.Objects.requireNonNull;

public class EventStore implements IEventStore {

    private static final Logger logger = LoggerFactory.getLogger("event-store");

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;

    private final TableIndex index;
    private final Streams streams;
    public final IEventLog eventLog;
    private final Projections projections;

    private EventStore(File rootDir) {
        this.index = new TableIndex(rootDir);
        this.projections = new Projections(new ProjectionExecutor(rootDir, this::appendSystemEvent));
        this.streams = new Streams(rootDir, LRU_CACHE_SIZE, index::version);
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer())
                .segmentSize(Size.MB.of(512))
                .name("event-log")
                .flushMode(FlushMode.MANUAL)
                .bufferPool(new SingleBufferThreadCachedPool(false))
                .checksumProbability(1)
                .disableCompaction()
                .compactionStrategy(new RecordCleanup(streams)));

        try {
            this.loadIndex();
//            this.loadStreams();
            this.loadProjections();
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(projections);
            IOUtils.closeQuietly(streams);
            IOUtils.closeQuietly(eventLog);
            throw new RuntimeException(e);
        }
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }


    private void loadIndex() {
        logger.info("Loading index");
        long start = System.currentTimeMillis();
        int loaded = 0;
        long p = 0;

        Map<EventRecord, Long> backwardsIndex = new HashMap<>(500000);
        try (LogIterator<EventRecord> iterator = eventLog.iterator(Direction.BACKWARD)) {

            while (iterator.hasNext()) {
                EventRecord next = iterator.next();
                if (IndexFlushed.TYPE.equals(next.type)) {
                    break;
                }
                backwardsIndex.put(next, iterator.position());
                if (++loaded % 50000 == 0) {
                    logger.info("Loaded {} index entries", loaded);
                }
            }

            backwardsIndex.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getValue)).forEach(e -> {
                EventRecord entry = e.getKey();
                long position = e.getValue();
                index.add(entry.streamName(), position);
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to load memIndex on position " + p, e);
        }

        logger.info("Loaded {} index entries in {}ms", loaded, (System.currentTimeMillis() - start));
    }

    private void loadProjections() {
        logger.info("Loading projections");
        long start = System.currentTimeMillis();

        Set<String> running = new HashSet<>();

        fromStream(StreamName.of(SystemStreams.PROJECTIONS))
                .when(ProjectionCreated.TYPE, ev -> projections.add(ProjectionCreated.from(ev)))
                .when(ProjectionUpdated.TYPE, ev -> projections.add(ProjectionCreated.from(ev)))
                .when(ProjectionDeleted.TYPE, ev -> projections.delete(ProjectionDeleted.from(ev).name))
                .when(ProjectionStarted.TYPE, ev -> running.add(ProjectionStarted.from(ev).name))
                .when(ProjectionResumed.TYPE, ev -> running.add(ProjectionResumed.from(ev).name))
                .when(ProjectionStopped.TYPE, ev -> running.remove(ProjectionStopped.from(ev).name))
                .when(ProjectionFailed.TYPE, ev -> running.remove(ProjectionFailed.from(ev).name))
                .match();


        int loaded = projections.all().size();
        logger.info("Loaded {} projections in {}ms", loaded, (System.currentTimeMillis() - start));
        if (loaded == 0) {
            logger.info("Creating system projections");
            for (Projection projection : this.projections.loadSystemProjections()) {
                createProjection(projection);
                projections.add(projection);
            }
        }

        projections.bootstrapProjections(this, running);
    }

    @Override
    public void cleanup() {
        eventLog.cleanup();
    }

    @Override
    public void compactIndex() {
        index.compact();
    }

    @Override
    public void createStream(String name) {
        createStream(name, -1, -1);
    }

    @Override
    public Collection<Projection> projections() {
        return new ArrayList<>(projections.all());
    }

    @Override
    public Projection projection(String name) {
        return projections.get(name);
    }

    @Override
    public synchronized Projection createProjection(String script) {
        Projection projection = projections.create(script);
        return createProjection(projection);
    }

    private synchronized Projection createProjection(Projection projection) {
        EventRecord eventRecord = ProjectionCreated.create(projection);
        this.appendSystemEvent(eventRecord);
        return projection;
    }

    @Override
    public synchronized Projection updateProjection(String name, String script) {
        Projection projection = projections.update(name, script);
        EventRecord eventRecord = ProjectionUpdated.create(projection);
        this.appendSystemEvent(eventRecord);

        return projection;
    }

    @Override
    public synchronized void deleteProjection(String name) {
        projections.delete(name);
        EventRecord eventRecord = ProjectionDeleted.create(name);
        this.appendSystemEvent(eventRecord);
    }

    @Override
    public synchronized void runProjection(String name) {
        projections.run(name, this);
        EventRecord eventRecord = ProjectionStarted.create(name);
        this.appendSystemEvent(eventRecord);
    }

    @Override
    public void resetProjection(String name) {
        projections.reset(name);
    }

    @Override
    public void stopProjectionExecution(String name) {
        projections.stop(name);
    }

    @Override
    public void disableProjection(String name) {
        Projection projection = projections.get(name);
        if(!projection.enabled) {
            return;
        }
        projection.enabled = false;
        EventRecord eventRecord = ProjectionUpdated.create(projection);
        this.appendSystemEvent(eventRecord);
    }

    @Override
    public void enableProjection(String name) {
        Projection projection = projections.get(name);
        if(projection.enabled) {
            return;
        }
        projection.enabled = true;
        EventRecord eventRecord = ProjectionUpdated.create(projection);
        this.appendSystemEvent(eventRecord);
    }

    @Override
    public Map<String, TaskStatus> projectionExecutionStatus(String name) {
        return projections.executionStatus(name);
    }

    @Override
    public Collection<Metrics> projectionExecutionStatuses() {
        return projections.executionStatuses();
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, maxCount, maxAge, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        StreamMetadata created = streams.create(stream, maxAge, maxCount, permissions, metadata);
        if (created == null) {
            throw new IllegalStateException("Stream '" + stream + "' already exist");
        }
        EventRecord eventRecord = StreamCreated.create(created);
        this.appendSystemEvent(eventRecord);
        return created;
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return streams.all().stream().map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        }).collect(Collectors.toList());
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.get(streamHash).map(meta -> {
            int version = streams.version(meta.hash);
            return StreamInfo.from(meta, version);
        });
    }

    //TODO remove ?? use only for log dump
    @Override
    public LogIterator<IndexEntry> scanIndex() {
        return index.scanner();
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        requireNonNull(stream, "Stream must be provided");
        int versionInclusive = stream.version() == NO_VERSION ? Range.START_VERSION : stream.version();
        StreamName start = StreamName.create(stream.name(), versionInclusive - 1);

        LogIterator<IndexEntry> indexIterator = index.indexedIterator(Map.of(start.hash(), start.version()));
        indexIterator = withMaxCountFilter(stream.hash(), indexIterator);
        IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
        EventLogIterator ageFilterIterator = withMaxAgeFilter(Set.of(stream.hash()), indexedLogIterator);
        return new LinkToResolveIterator(ageFilterIterator, this::resolve);
    }


    @Override
    public EventLogIterator fromStreams(String streamPattern) {
        Set<String> eventStreams = streams.streamMatching(streamPattern);
        if (eventStreams.isEmpty()) {
            return EventLogIterator.empty();
        }
        return fromStreams(eventStreams.stream().map(StreamName::of).collect(Collectors.toSet()));
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streamNames) {
        if (streamNames.size() == 1) {
            return fromStream(streamNames.iterator().next());
        }

        Map<Long, Integer> streamVersions = streamNames.stream()
                .filter(sn -> StringUtils.nonBlank(sn.name()))
                .collect(Collectors.toMap(sn -> streams.hashOf(sn.name()), StreamName::version));

        Set<Long> hashes = new HashSet<>(streamVersions.keySet());

        TableIndex.IndexIterator indexIterator = index.indexedIterator(streamVersions);
        IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
        EventLogIterator ageFilterIterator = withMaxAgeFilter(hashes, indexedLogIterator);
        return new LinkToResolveIterator(ageFilterIterator, this::resolve);
    }

    @Override
    public int version(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.version(streamHash);
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        LogIterator<EventRecord> logIterator = eventLog.iterator(Direction.FORWARD);
        NonIndexedLogIterator nonIndexedLogIterator = new NonIndexedLogIterator(logIterator);
        return new EventPolicyFilterIterator(nonIndexedLogIterator, linkToPolicy, systemEventPolicy);
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        requireNonNull(lastEvent, "last event must be provided");
        Optional<IndexEntry> indexEntry = index.get(lastEvent.hash(), lastEvent.version());
        IndexEntry entry = indexEntry.orElseThrow(() -> new IllegalArgumentException("No index entry found for " + lastEvent));
        LogIterator<EventRecord> logIterator = eventLog.iterator(Direction.FORWARD, entry.position);
        NonIndexedLogIterator nonIndexedLogIterator = new NonIndexedLogIterator(logIterator);
        return new EventPolicyFilterIterator(nonIndexedLogIterator, linkToPolicy, systemEventPolicy);
    }

    @Override
    public synchronized EventRecord linkTo(String stream, EventRecord event) {
        if (event.isLinkToEvent()) {
            //resolve event
            event = get(event.streamName());
        }
        EventRecord linkTo = LinkTo.create(stream, StreamName.from(event));
        return this.appendSystemEvent(linkTo);
    }

    @Override
    public synchronized EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
        if (LinkTo.TYPE.equals(sourceType)) {
            //resolve event
            EventRecord resolvedEvent = get(source);
            StreamName resolvedStream = resolvedEvent.streamName();
            EventRecord linkTo = LinkTo.create(dstStream, resolvedStream);
            return this.appendSystemEvent(linkTo);
        }
        EventRecord linkTo = LinkTo.create(dstStream, source);
        return this.appendSystemEvent(linkTo);

    }

    @Override
    public EventRecord get(StreamName stream) {
        if (!stream.hasVersion()) {
            throw new IllegalArgumentException("Version must be greater than " + NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(stream.hash(), stream.version());
        if (!indexEntry.isPresent()) {
            //TODO improve this to a non exception response
            throw new RuntimeException("IndexEntry not found for " + stream);
        }

        return get(indexEntry.get());
    }

    @Override
    public EventRecord get(IndexEntry indexEntry) {
        requireNonNull(indexEntry, "IndexEntry mus not be null");

        EventRecord record = eventLog.get(indexEntry.position);
        return resolve(record);
    }

    @Override
    public EventRecord resolve(EventRecord record) {
        if (record.isLinkToEvent()) {
            return get(record.streamName());
        }
        return record;
    }

    private void validateEvent(EventRecord event) {
        requireNonNull(event, "Event must be provided");
        StringUtils.requireNonBlank(event.stream, "closeableStream must be provided");
        StringUtils.requireNonBlank(event.type, "Type must be provided");
        if (event.stream.startsWith(StreamName.SYSTEM_PREFIX)) {
            throw new IllegalArgumentException("Stream cannot start with " + StreamName.SYSTEM_PREFIX);
        }
    }

    @Override
    public EventRecord append(EventRecord event) {
        return append(event, NO_VERSION);
    }

    @Override
    public synchronized EventRecord append(EventRecord event, int expectedVersion) {
        validateEvent(event);

        StreamMetadata metadata = getOrCreateStream(event.stream);
        return appendInternal(metadata, event, expectedVersion);
    }

    private EventRecord appendSystemEvent(EventRecord event) {
        StreamMetadata metadata = getOrCreateStream(event.stream);
        return appendInternal(metadata, event, NO_VERSION);
    }

    private  EventRecord appendInternal(StreamMetadata streamMetadata, EventRecord event, int expectedVersion) {
        if (streamMetadata == null) {
            throw new IllegalArgumentException("EventStream cannot be null");
        }

        StreamName stream = event.streamName();
        long streamHash = stream.hash();

        if (streamMetadata.name.equals(event.stream) && streamMetadata.hash != streamHash) {
            //TODO improve ??
            throw new IllegalStateException("Hash collision of closeableStream: " + event.stream + " with existing name: " + streamMetadata.name);
        }

        int version = streams.tryIncrementVersion(streamHash, expectedVersion);

        var record = new EventRecord(event.stream, event.type, version, System.currentTimeMillis(), event.body, event.metadata);

        long position = eventLog.append(record);
        var flushInfo = index.add(streamHash, version, position);
        if (flushInfo != null) {
            var indexFlushedEvent = IndexFlushed.create(position, flushInfo.timeTaken, flushInfo.entries);
            this.appendSystemEvent(indexFlushedEvent);
        }

        return record;
    }

    private StreamMetadata getOrCreateStream(String stream) {
        return streams.createIfAbsent(stream, created -> {
            EventRecord eventRecord = StreamCreated.create(created);
            this.appendInternal(created, eventRecord, NO_VERSION);
        });
    }

    private LogIterator<IndexEntry> withMaxCountFilter(long streamHash, LogIterator<IndexEntry> iterator) {
        return streams.get(streamHash)
                .map(stream -> stream.maxCount)
                .filter(maxCount -> maxCount > 0)
                .map(maxCount -> MaxCountFilteringIterator.of(maxCount, streams.version(streamHash), iterator))
                .orElse(iterator);
    }

    private EventLogIterator withMaxAgeFilter(Set<Long> streamHashes, EventLogIterator iterator) {
        Map<String, Long> metadataMap = streamHashes.stream()
                .map(streams::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(StreamMetadata::name, meta -> meta.maxAge));

        return new MaxAgeFilteringIterator(metadataMap, iterator);
    }

    @Override
    public synchronized void close() {
        index.close();
        eventLog.close();
        streams.close();
        projections.close();
    }

}