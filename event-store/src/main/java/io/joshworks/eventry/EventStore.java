package io.joshworks.eventry;

import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.StreamTruncated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventLog;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.log.RecordCleanup;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.eventry.writer.EventWriter;
import io.joshworks.eventry.writer.Writer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;
import static java.util.Objects.requireNonNull;

public class EventStore implements IEventStore {

    private static final Logger logger = LoggerFactory.getLogger("event-store");

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;
    private static final int WRITE_QUEUE_SIZE = 1000000;


    private final TableIndex index;
    private final Streams streams;
    private final IEventLog eventLog;
    private final EventWriter eventWriter;

    private EventStore(File rootDir) {
        long start = System.currentTimeMillis();
        this.index = new TableIndex(rootDir, this::fetchMetadata, this::onIndexWrite, 50000, 5, true);
        this.streams = new Streams(rootDir, LRU_CACHE_SIZE, index::version);
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer())
                .segmentSize(Size.MB.of(512))
                .name("event-log")
                .flushMode(FlushMode.MANUAL)
                .storageMode(StorageMode.MMAP)
                .bufferPool(new SingleBufferThreadCachedPool(false))
                .checksumProbability(1)
                .disableCompaction()
                .compactionStrategy(new RecordCleanup(streams)));

        this.eventWriter = new EventWriter(streams, eventLog, index, WRITE_QUEUE_SIZE);
        try {
            this.loadIndex();
            this.initializeStreamsStreams();
            logger.info("Started event store in {}ms", (System.currentTimeMillis() - start));
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(streams);
            IOUtils.closeQuietly(eventLog);
            IOUtils.closeQuietly(eventWriter);
            throw new RuntimeException(e);
        }
    }

    private void initializeStreamsStreams() {
        Optional<IndexEntry> entry = index.get(streams.hashOf(SystemStreams.STREAMS), 0);
        if (entry.isPresent()) {
            logger.info("System stream already initialized");
            return;
        }

        logger.info("Initializing system streams");
        Future<Void> task1 = eventWriter.queue(writer -> {
            StreamMetadata metadata = streams.create(SystemStreams.STREAMS);
            EventRecord record = StreamCreated.create(metadata);
            writer.append(record, NO_VERSION, metadata);
        });

        Future<Void> task2 = eventWriter.queue(writer -> {
            StreamMetadata metadata = streams.create(SystemStreams.PROJECTIONS);
            EventRecord record = StreamCreated.create(metadata);
            writer.append(record, NO_VERSION, metadata);
        });

        Future<Void> task3 = eventWriter.queue(writer -> {
            StreamMetadata metadata = streams.create(SystemStreams.INDEX);
            EventRecord record = StreamCreated.create(metadata);
            writer.append(record, NO_VERSION, metadata);
        });

        Threads.awaitFor(task1);
        Threads.awaitFor(task2);
        Threads.awaitFor(task3);

    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }

    private StreamMetadata fetchMetadata(long stream) {
        Optional<StreamMetadata> streamMetadata = streams.get(stream);
        if (!streamMetadata.isPresent()) {
            throw new IllegalArgumentException("Could not find stream metadata for " + stream);
        }
        return streamMetadata.get();
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

    @Override
    public EventRecord append(EventRecord event) {
        return append(event, NO_VERSION);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        validateEvent(event);
        Future<EventRecord> future = eventWriter.queue(writer -> {
            StreamMetadata metadata = getOrCreateStream(writer, event.stream);
            return writer.append(event, expectedVersion, metadata);
        });
        return Threads.awaitFor(future);
    }

    @Override
    public void compact() {
        index.compact();
        eventLog.compact();
    }

    @Override
    public void createStream(String name) {
        createStream(name, -1, -1);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, maxCount, maxAge, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        Future<StreamMetadata> future = eventWriter.queue(writer -> {
            StreamMetadata created = streams.create(stream, maxAge, maxCount, permissions, metadata);
            if (created == null) {
                throw new IllegalStateException("Stream '" + stream + "' already exist");
            }
            EventRecord eventRecord = StreamCreated.create(created);
            StreamMetadata streamsStreamMeta = streams.get(SystemStreams.STREAMS).get();
            writer.append(eventRecord, -2, streamsStreamMeta);
            return created;
        });

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

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

    @Override
    public void truncate(String stream, int version) {
        Future<Void> op = eventWriter.queue(writer -> {
            StreamMetadata metadata = streams.get(stream).orElseThrow(() -> new IllegalArgumentException("Invalid stream"));
            streams.truncate(metadata, version);
            EventRecord eventRecord = StreamTruncated.create(metadata.name, version);
            writer.append(eventRecord, -1, metadata);
        });

        Threads.awaitFor(op);
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        requireNonNull(stream, "Stream must be provided");
        int version = stream.version();
        String name = stream.name();
        long hash = stream.hash();

        return streams.get(hash).map(metadata -> {
            int startVersion = version == NO_VERSION ? Range.START_VERSION : version;
            int finalVersion = metadata.truncated() && startVersion < metadata.truncateBefore ? metadata.truncateBefore : startVersion;
            StreamName start = StreamName.of(name, finalVersion - 1);

            LogIterator<IndexEntry> indexIterator = index.indexedIterator(Map.of(start.hash(), start.version()));
            indexIterator = withMaxCountFilter(hash, indexIterator);
            IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
            EventLogIterator ageFilterIterator = withMaxAgeFilter(Set.of(hash), indexedLogIterator);
            return (EventLogIterator) new LinkToResolveIterator(ageFilterIterator, this::resolve);

        }).orElseGet(EventLogIterator::empty);

    }

    @Override
    public EventLogIterator fromStreams(String streamPattern) {
        Set<String> eventStreams = streams.streamMatching(streamPattern);
        if (eventStreams.isEmpty()) {
            return EventLogIterator.empty();
        }
        return fromStreams(eventStreams.stream().map(StreamName::parse).collect(Collectors.toSet()));
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streamNames) {
        if (streamNames.size() == 1) {
            return fromStream(streamNames.iterator().next());
        }

        Map<Long, Integer> streamVersions = streamNames.stream()
                .filter(sn -> StringUtils.nonBlank(sn.name()))
                .collect(Collectors.toMap(StreamName::hash, StreamName::version));

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
    public EventRecord linkTo(String stream, final EventRecord event) {
        Future<EventRecord> future = eventWriter.queue(writer -> {
            EventRecord resolved = resolve(event);
            StreamMetadata metadata = getOrCreateStream(writer, stream);
            EventRecord linkTo = LinkTo.create(stream, StreamName.from(resolved));
            return writer.append(linkTo, NO_VERSION, metadata); // TODO expected version for LinkTo
        });

        return Threads.awaitFor(future);
    }

    @Override
    public EventRecord linkTo(String stream, StreamName tgtEvent, String sourceType) {
        Future<EventRecord> future = eventWriter.queue(writer -> {
            EventRecord linkTo = LinkTo.create(stream, tgtEvent);
            StreamMetadata metadata = getOrCreateStream(writer, stream);
            if (LinkTo.TYPE.equals(sourceType)) {
                EventRecord resolvedEvent = get(tgtEvent);
                StreamName resolvedStream = resolvedEvent.streamName();
                linkTo = LinkTo.create(stream, resolvedStream);
            }
            return writer.append(linkTo, NO_VERSION, metadata);
        });
        return Threads.awaitFor(future);
    }

    private StreamMetadata getOrCreateStream(Writer writer, String stream) {
        return streams.createIfAbsent(stream, created -> {
            EventRecord eventRecord = StreamCreated.create(created);
            StreamMetadata metadata = streams.get(SystemStreams.STREAMS).get();
            writer.append(eventRecord, -1, metadata);
        });
    }

    @Override
    public EventRecord get(StreamName stream) {
        EventRecord record = getInternal(stream);
        return resolve(record);
    }

    //get WITHOUT RESOLVING
    public EventRecord getInternal(StreamName stream) {
        if (!stream.hasVersion()) {
            throw new IllegalArgumentException("Version must be greater than " + NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(stream.hash(), stream.version());
        if (!indexEntry.isPresent()) {
            //TODO improve this to a non exception response
            throw new RuntimeException("IndexEntry not found for " + stream);
        }

        IndexEntry ie = indexEntry.get();
        return eventLog.get(ie.position);
    }

    private EventRecord resolve(EventRecord record) {
        if (record.isLinkToEvent()) {
            LinkTo linkTo = LinkTo.from(record);
            StreamName targetEvent = StreamName.of(linkTo.stream, linkTo.version);
            EventRecord resolved = getInternal(targetEvent);
            if (resolved.isLinkToEvent()) {
                throw new IllegalStateException("Found second level LinkTo event");
            }
            return resolved;
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
        if (LinkTo.TYPE.equals(event.type)) {
            throw new IllegalArgumentException("Stream type cannot be " + LinkTo.TYPE);
        }
    }

    private void onIndexWrite(TableIndex.FlushInfo flushInfo) {
        eventWriter.queue(writer -> {
            var indexFlushedEvent = IndexFlushed.create(flushInfo.timeTaken, flushInfo.entries);
            StreamMetadata metadata = getOrCreateStream(writer, indexFlushedEvent.stream);
            writer.append(indexFlushedEvent, NO_VERSION, metadata);
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
        //Order matters
        index.close(); //#1
        eventWriter.close();//#2
        eventLog.close();
        streams.close();
    }

}