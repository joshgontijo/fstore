package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.StreamTruncated;
import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.IndexIterator;
import io.joshworks.eventry.log.EventLog;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.writer.EventWriter;
import io.joshworks.eventry.writer.Writer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.LinkTo;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.SequentialNaming;
import io.joshworks.fstore.log.segment.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static io.joshworks.fstore.es.shared.EventId.NO_EXPECTED_VERSION;
import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static io.joshworks.fstore.es.shared.utils.StringUtils.requireNonBlank;
import static java.util.Objects.requireNonNull;

public class EventStore implements IEventStore {

    private static final Logger logger = LoggerFactory.getLogger("event-store");

    private static final int WRITE_QUEUE_SIZE = 1000000;
    private static final int INDEX_FLUSH_THRESHOLD = 1000000;
    private static final int STREAMS_FLUSH_THRESHOLD = 50000;

    //TODO externalize
    private final Cache<Long, Integer> versionCache = Cache.lruCache(5000000, -1);
    //    private final Cache<Long, StreamMetadata> streamCache = Cache.lruCache(100000, 120);
    private final Cache<Long, StreamMetadata> streamCache = Cache.lruCache(5000, -1);

    public final Index index;
    public final Streams streams; //TODO fix test to make this protected
    private final IEventLog eventLog;
    private final EventWriter eventWriter;

    private final Set<StreamListener> streamListeners = ConcurrentHashMap.newKeySet();
    private final StreamMetadata streamsMetadata;

    protected EventStore(File rootDir) {
        long start = System.currentTimeMillis();
        this.streams = new Streams(rootDir, STREAMS_FLUSH_THRESHOLD, streamCache);
        this.index = new Index(rootDir, INDEX_FLUSH_THRESHOLD, versionCache, streams::get);
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer())
                .segmentSize(Size.MB.of(512))
                .name("event-log")
                .flushMode(FlushMode.MANUAL)
                .storageMode(StorageMode.MMAP)
                .directBufferPool()
                .checksumProbability(0.1)
                .namingStrategy(new SequentialNaming(rootDir))
                .open());
        //TODO log compaction (prune) not fully implemented
//                .compactionThreshold(1)
//                .compactionStrategy(new RecordCleanup(streams, index)));

        this.eventWriter = new EventWriter(eventLog, index, WRITE_QUEUE_SIZE);
        try {
            if (!this.initializeSystemStreams()) {
                this.loadIndex();
            }
            streamsMetadata = streams.get(SystemStreams.STREAMS);
            logger.info("Started event store in {}ms", (System.currentTimeMillis() - start));
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(streams);
            IOUtils.closeQuietly(eventLog);
            IOUtils.closeQuietly(eventWriter);
            throw new RuntimeException(e);
        }
    }

    private boolean initializeSystemStreams() {
        StreamMetadata entry = streams.get(SystemStreams.STREAMS_HASH);
        if (entry != null) {
            logger.info("System stream already initialized");
            return false;
        }

        logger.info("Initializing system streams");
        StreamMetadata metadata = streams.create(SystemStreams.STREAMS);
        logger.info("Created {}", SystemStreams.STREAMS);

        Future<Void> task = eventWriter.queue(writer -> {
            writer.append(StreamCreated.create(metadata), NO_VERSION, metadata, false);

            StreamMetadata projectionsMetadata = streams.create(SystemStreams.PROJECTIONS);
            writer.append(StreamCreated.create(projectionsMetadata), 0, metadata, false);
            logger.info("Created {}", SystemStreams.PROJECTIONS);

            StreamMetadata indexMetadata = streams.create(SystemStreams.INDEX);
            writer.append(StreamCreated.create(indexMetadata), 1, metadata, false);
            logger.info("Created {}", SystemStreams.INDEX);

            return null;
        });

        Threads.waitFor(task);
        return true;
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }

    private void loadIndex() {
        logger.info("Loading index");
        long start = System.currentTimeMillis();

        long lastFlushedPos = Log.START;
        Map<EventRecord, Long> backwardsIndex = new HashMap<>();
        try (LogIterator<EventRecord> iterator = eventLog.iterator(Direction.BACKWARD)) {
            int loaded = 0;
            while (iterator.hasNext()) {
                EventRecord next = iterator.next();
                long logPos = iterator.position();
                if (IndexFlushed.TYPE.equals(next.type)) {
                    lastFlushedPos = IndexFlushed.from(next).logPosition;
                }

                if (logPos < lastFlushedPos) {
                    break;
                }
                backwardsIndex.put(next, logPos);
                if (++loaded % 50000 == 0) {
                    logger.info("Loaded {} index entries", loaded);
                }
            }

            final long lastPos = lastFlushedPos;
            backwardsIndex.entrySet().stream().filter(kv -> kv.getValue().compareTo(lastPos) >= 0).sorted(Comparator.comparingLong(Map.Entry::getValue)).forEach(e -> {
                EventRecord entry = e.getKey();
                long position = e.getValue();
                EventId eventId = entry.streamName();
                index.add(StreamHasher.hash(entry.stream), eventId.version(), position);
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to load memIndex on position", e);
        }

        logger.info("Loaded {} index entries in {}ms", index.size(), (System.currentTimeMillis() - start));
    }

    public Future<Void> append(List<EventRecord> events) {
        return eventWriter.queue(writer -> {
            for (EventRecord record : events) {
                appendInternal(writer, NO_EXPECTED_VERSION, record);
            }
            return null;
        });
    }

    public CompletableFuture<EventRecord> appendAsync(EventRecord event) {
        return appendAsync(event, NO_EXPECTED_VERSION);
    }

    public CompletableFuture<EventRecord> appendAsync(EventRecord event, int expectedVersion) {
        validateEvent(event);
        return eventWriter.queue(writer -> appendInternal(writer, expectedVersion, event));
    }

    @Override
    public EventRecord append(EventRecord event) {
        Future<EventRecord> future = appendAsync(event, NO_EXPECTED_VERSION);
        return Threads.waitFor(future);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        validateEvent(event);
        Future<EventRecord> future = eventWriter.queue(writer -> appendInternal(writer, expectedVersion, event));
        return Threads.waitFor(future);
    }

    @Override
    public void compact() {
        index.compact();
        eventLog.compact();
    }

    @Override
    public StreamMetadata createStream(String name) {
        return createStream(name, NO_MAX_COUNT, NO_MAX_AGE, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, int maxAgeSec, Map<String, Integer> acl, Map<String, String> metadata) {
        Future<StreamMetadata> task = eventWriter.queue(writer -> {
            StreamMetadata created = streams.create(stream, maxCount, maxAgeSec, acl, metadata);
            if (created == null) {
                throw new IllegalStateException("Stream '" + stream + "' already exist");
            }
            EventRecord eventRecord = StreamCreated.create(created);
            writer.append(eventRecord, NO_EXPECTED_VERSION, streamsMetadata, false);
            return created;
        });

        StreamMetadata created = Threads.waitFor(task);
        for (StreamListener streamListener : streamListeners) {
            streamListener.onStreamCreated(created);
        }
        return created;
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return streams.all().stream().map(meta -> {
            int version = index.version(meta.hash);
            return StreamInfo.from(meta, version);
        }).collect(Collectors.toList());
    }

    @Override
    public Set<Long> streams() {
        return streams.allHashes();
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        long streamHash = StreamHasher.hash(stream);
        return Optional.ofNullable(streams.get(streamHash)).map(meta -> {
            int version = index.version(meta.hash);
            return StreamInfo.from(meta, version);
        });
    }

    @Override
    public void truncate(String stream, int fromVersionInclusive) {
        Future<StreamMetadata> op = eventWriter.queue(writer -> {
            StreamMetadata metadata = Optional.ofNullable(streams.get(stream)).orElseThrow(() -> new IllegalArgumentException("Invalid stream"));
            int currentVersion = index.version(metadata.hash);
            StreamMetadata truncatedMetadata = streams.truncate(metadata, currentVersion, fromVersionInclusive);
            EventRecord eventRecord = StreamTruncated.create(metadata.name, fromVersionInclusive);

            StreamMetadata streamsMetadata = streams.get(SystemStreams.STREAMS);
            writer.append(eventRecord, NO_EXPECTED_VERSION, streamsMetadata, false);
            return truncatedMetadata;
        });

        StreamMetadata truncated = Threads.waitFor(op);
        for (StreamListener streamListener : streamListeners) {
            streamListener.onStreamTruncated(truncated);
        }
    }

    @Override
    public EventStoreIterator fromStream(EventId eventId) {
        requireNonNull(eventId, "Stream must be provided");
        return fromStreams(EventMap.from(eventId));
    }

    @Override
    public EventStoreIterator fromStreams(EventMap checkpoint, Set<String> streamPatterns) {
        if (streamPatterns == null || streamPatterns.isEmpty()) {
            throw new IllegalArgumentException("Stream pattern must be provided");
        }
        Set<Long> longs = streams.matchStreamHash(streamPatterns);
        EventMap fromPattern = EventMap.of(longs);
        EventMap merged = checkpoint.merge(fromPattern);
        EventMap resolved = updateWithMinVersion(merged);

        IndexIterator indexIterator = index.iterator(resolved, streamPatterns);
        IndexListenerRemoval listenerRemoval = new IndexListenerRemoval(streamListeners, indexIterator);

        return new IndexedLogIterator(listenerRemoval, eventLog, this::resolve);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap streams) {
        if (streams.isEmpty()) {
            throw new IllegalArgumentException("At least one stream must be provided");
        }
        streams = updateWithMinVersion(streams);

        IndexIterator indexIterator = index.iterator(streams);
        IndexListenerRemoval listenerRemoval = new IndexListenerRemoval(streamListeners, indexIterator);

        return new IndexedLogIterator(listenerRemoval, eventLog, this::resolve);
    }

    @Override
    public int version(String stream) {
        return index.version(stream);
    }

    @Override
    public int count(String stream) {
        return Optional.ofNullable(streams.get(stream)).map(m -> {
            int version = version(stream);
            return m.truncated > NO_TRUNCATE ? version - m.truncated : version + 1;
        }).orElse(0);
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return fromAll(linkToPolicy, systemEventPolicy, null);
    }

    @Override //checkpoint is the actual log position, using EventId to keep consistent
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId checkpoint) {
        long position = getStartPosition(checkpoint);
        LogIterator<EventRecord> logIterator = eventLog.iterator(Direction.FORWARD, position);
        return new EventLogIterator(logIterator, this::resolve, linkToPolicy, systemEventPolicy);
    }

    private long getStartPosition(EventId lastEvent) {
        return Optional.ofNullable(lastEvent)
                .flatMap(ev -> index.get(StreamHasher.hash(lastEvent.name()), ev.version()))
                .map(ie -> ie.position)
                .orElse(Log.START);
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        Future<EventRecord> future = eventWriter.queue(writer -> {
            EventRecord resolved = resolve(event);
            EventRecord linkTo = LinkTo.create(stream, EventId.of(resolved.stream, resolved.version));
            return appendInternal(writer, NO_EXPECTED_VERSION, linkTo); // TODO expected version for LinkTo
        });

        return Threads.waitFor(future);
    }

    @Override
    public EventRecord linkTo(String srcStream, EventId tgtEvent, String sourceType) {
        Future<EventRecord> future = eventWriter.queue(writer -> {
            EventRecord linkTo = LinkTo.create(srcStream, tgtEvent);
            if (LinkTo.TYPE.equals(sourceType)) {
                EventRecord resolvedEvent = get(tgtEvent);
                EventId resolvedStream = resolvedEvent.streamName();
                linkTo = LinkTo.create(srcStream, resolvedStream);
            }
            return appendInternal(writer, NO_EXPECTED_VERSION, linkTo); // TODO expected version for LinkTo
        });
        return Threads.waitFor(future);
    }

    private EventMap updateWithMinVersion(EventMap eventMap) {
        for (Map.Entry<Long, Integer> kv : eventMap.entrySet()) {
            long stream = kv.getKey();
            int version = kv.getValue();
            StreamMetadata metadata = streams.get(stream);
            if (metadata == null) {
                return EventMap.of(stream, version);
            }
            int minVersion = metadata.truncated() && version < metadata.truncated ? metadata.truncated : version;
            kv.setValue(minVersion);
        }
        return eventMap;
    }

    private EventRecord appendInternal(Writer writer, int expectedVersion, EventRecord record) {
        long hash = StreamHasher.hash(record.stream);
        StreamMetadata metadata = streams.get(hash);
        if (metadata == null) {
            metadata = streams.create(record.stream);
            appendStreamCreated(writer, metadata);
            return writer.append(record, expectedVersion, metadata, true);
        } else {
            return writer.append(record, expectedVersion, metadata, false);
        }
    }

    private void appendStreamCreated(Writer writer, StreamMetadata created) {
        EventRecord eventRecord = StreamCreated.create(created);
        //FIXME THIS ADDS UNNECESSARY LOAD ON MEMORY, REMOVE AND PASS AN ARGUMENT TO WRITER ON WHETHER THIS IS A NEW EVENT SO IT DOESN'T QUERY THE EVENT VERSION
        versionCache.add(created.hash, NO_VERSION); //cache, so when Writer is called the info will be available
        writer.append(eventRecord, NO_EXPECTED_VERSION, streamsMetadata, false);
        for (StreamListener listener : streamListeners) {
            listener.onStreamCreated(created);
        }
    }

    @Override
    public EventRecord get(EventId stream) {
        EventRecord record = getInternal(stream);
        return resolve(record);
    }

    //GET WITHOUT RESOLVING
    private EventRecord getInternal(EventId stream) {
        if (!stream.hasVersion()) {
            throw new IllegalArgumentException("Version must be greater than " + NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(StreamHasher.hash(stream.name()), stream.version());
        if (indexEntry.isEmpty()) {
            return null;
        }

        IndexEntry ie = indexEntry.get();
        return eventLog.get(ie.position);
    }

    protected EventRecord resolve(EventRecord record) {
        if (record == null) {
            return null;
        }
        if (record.isLinkToEvent()) {
            LinkTo linkTo = LinkTo.from(record);
            EventId targetEvent = EventId.of(linkTo.stream, linkTo.version);
            EventRecord resolved = getInternal(targetEvent);
            if (resolved != null && resolved.isLinkToEvent()) {
                throw new IllegalStateException("Found second level LinkTo event");
            }
            return resolved;
        }
        return record;
    }

    private void validateEvent(EventRecord event) {
        requireNonNull(event, "Event must be provided");
        requireNonBlank(event.stream, "closeableStream must be provided");
        requireNonBlank(event.type, "Type must be provided");
        if (event.stream.startsWith(EventId.SYSTEM_PREFIX)) {
            throw new IllegalArgumentException("Stream cannot start with " + EventId.SYSTEM_PREFIX);
        }
        if (LinkTo.TYPE.equals(event.type)) {
            throw new IllegalArgumentException("Stream type cannot be " + LinkTo.TYPE);
        }
    }

    //TODO circular data flow between index and eventWriter might cause data to be lost in very specific scenarios
    @Override
    public synchronized void close() {
        //Order matters
        index.close(); //#1
        eventWriter.close();//#2
        eventLog.close();
        streams.close();
    }

}