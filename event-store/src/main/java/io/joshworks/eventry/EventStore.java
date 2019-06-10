package io.joshworks.eventry;

import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.StreamTruncated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.Checkpoint;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.IndexIterator;
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
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.SequentialNaming;
import io.joshworks.fstore.log.segment.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.joshworks.eventry.log.EventRecord.NO_EXPECTED_VERSION;
import static io.joshworks.eventry.log.EventRecord.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static java.util.Objects.requireNonNull;

public class EventStore implements IEventStore {

    private static final Logger logger = LoggerFactory.getLogger("event-store");

    //TODO expose
    private static final int LRU_CACHE_SIZE = 1000000;
    private static final int WRITE_QUEUE_SIZE = 1000000;
    private static final int INDEX_FLUSH_THRESHOLD = 50000;
    private static final int INDEX_MAX_WRITE_QUEUE_SIZE = 5;


    private final TableIndex index;
    public final Streams streams; //TODO fix test to make this private
    private final IEventLog eventLog;
    private final EventWriter eventWriter;

    private EventStore(File rootDir) {
        long start = System.currentTimeMillis();
        this.index = new TableIndex(rootDir, this::fetchMetadata, this::onIndexFlushed, INDEX_FLUSH_THRESHOLD, INDEX_MAX_WRITE_QUEUE_SIZE, new SnappyCodec());
        this.streams = new Streams(rootDir, LRU_CACHE_SIZE, index::version);
        this.eventLog = new EventLog(LogAppender.builder(rootDir, new EventSerializer())
                .segmentSize(Size.MB.of(512))
                .name("event-log")
                .flushMode(FlushMode.MANUAL)
                .storageMode(StorageMode.MMAP)
                .bufferPool(new LocalGrowingBufferPool(false))
                .checksumProbability(1)
                .disableCompaction()
                .namingStrategy(new SequentialNaming(rootDir))
                .compactionStrategy(new RecordCleanup(streams)));

        long sequence = initialSequence();
        logger.info("Initial sequence: {}", sequence);
        this.eventWriter = new EventWriter(streams, eventLog, index, WRITE_QUEUE_SIZE, sequence);
        try {
            if (!this.initializeSystemStreams()) {
                this.loadIndex();
            }
            logger.info("Started event store in {}ms", (System.currentTimeMillis() - start));
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(streams);
            IOUtils.closeQuietly(eventLog);
            IOUtils.closeQuietly(eventWriter);
            throw new RuntimeException(e);
        }
    }

    private long initialSequence() {
        try (LogIterator<EventRecord> iterator = eventLog.iterator(Direction.BACKWARD)) {
            if (iterator.hasNext()) {
                return iterator.next().sequence + 1;
            }
            return 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean initializeSystemStreams() {
        Optional<StreamMetadata> entry = streams.get(streams.hashOf(SystemStreams.STREAMS));
        if (entry.isPresent()) {
            logger.info("System stream already initialized");
            return false;
        }

        logger.info("Initializing system streams");
        StreamMetadata metadata = streams.create(SystemStreams.STREAMS);
        Future<Void> task = eventWriter.queue(writer -> {
            writer.append(StreamCreated.create(metadata), NO_VERSION, metadata);

            StreamMetadata projectionsMetadata = streams.create(SystemStreams.PROJECTIONS);
            writer.append(StreamCreated.create(projectionsMetadata), 0, metadata);

            StreamMetadata indexMetadata = streams.create(SystemStreams.INDEX);
            writer.append(StreamCreated.create(indexMetadata), 1, metadata);
        });

        Threads.waitFor(task);
        return true;
    }

    public static EventStore open(File rootDir) {
        return new EventStore(rootDir);
    }

    private StreamMetadata tryGetMetadata(String stream) {
        return fetchMetadata(streams.hashOf(stream));
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
                StreamName streamName = entry.streamName();
                index.add(streamName.hash(), streamName.version(), position);
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to load memIndex on position", e);
        }

        logger.info("Loaded {} index entries in {}ms", index.size(), (System.currentTimeMillis() - start));
    }

    //replication
    public Future<EventRecord> add(EventRecord record) {
        return eventWriter.queue(writer -> {
            StreamMetadata metadata = getOrCreateStream(writer, record.stream);
            return writer.append(record, NO_EXPECTED_VERSION, metadata);
        });
    }

    public Future<Void> append(List<EventRecord> events) {
        return eventWriter.queue(writer -> {
            for (EventRecord record : events) {
                StreamMetadata metadata = getOrCreateStream(writer, record.stream);
                writer.append(record, NO_EXPECTED_VERSION, metadata);
            }
        });
    }

    @Override
    public EventRecord append(EventRecord event) {
        return append(event, NO_EXPECTED_VERSION);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        validateEvent(event);
        Future<EventRecord> future = eventWriter.queue(writer -> {
            StreamMetadata metadata = getOrCreateStream(writer, event.stream);
            return writer.append(event, expectedVersion, metadata);
        });
        return Threads.waitFor(future);
    }

    @Override
    public void compact() {
        index.compact();
        eventLog.compact();
    }

    @Override
    public void createStream(String name) {
        createStream(name, NO_MAX_COUNT, NO_MAX_AGE);
    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {
        createStream(name, maxCount, maxAge, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        Future<StreamMetadata> task = eventWriter.queue(writer -> {
            StreamMetadata created = streams.create(stream, maxAge, maxCount, acl, metadata);
            if (created == null) {
                throw new IllegalStateException("Stream '" + stream + "' already exist");
            }
            EventRecord eventRecord = StreamCreated.create(created);
            StreamMetadata streamsStreamMeta = streams.get(SystemStreams.STREAMS).get();
            writer.append(eventRecord, NO_EXPECTED_VERSION, streamsStreamMeta);
            return created;
        });

        return Threads.waitFor(task);

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
    public void truncate(String stream, int fromVersionInclusive) {
        Future<Void> op = eventWriter.queue(writer -> {
            StreamMetadata metadata = streams.get(stream).orElseThrow(() -> new IllegalArgumentException("Invalid stream"));
            streams.truncate(metadata, fromVersionInclusive);
            EventRecord eventRecord = StreamTruncated.create(metadata.name, fromVersionInclusive);

            StreamMetadata streamsMetadata = streams.get(SystemStreams.STREAMS).get();
            writer.append(eventRecord, NO_EXPECTED_VERSION, streamsMetadata);
        });

        Threads.waitFor(op);
    }

    @Override
    public EventLogIterator fromStream(StreamName streamName) {
        requireNonNull(streamName, "Stream must be provided");
        int version = streamName.version();
        long hash = streamName.hash();

        //TODO this must be applied to fromStreams and fromStreamsPATTERN
        int startVersion = streams.get(hash).map(metadata -> metadata.truncated() && version < metadata.truncated ? metadata.truncated : version).orElse(version);

        LogIterator<IndexEntry> indexIterator = index.indexedIterator(Checkpoint.of(hash, startVersion));
        IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
        return createEventLogIterator(this::tryGetMetadata, indexedLogIterator);
    }

    //TODO this requires another method that accepts checkpoint
    @Override
    public EventLogIterator fromStreams(String streamPrefix, boolean ordered) {
        if (StringUtils.isBlank(streamPrefix)) {
            throw new IllegalArgumentException("stream prefix must not be empty");
        }

        Set<Long> longs = streams.matchStreamHash(streamPrefix);
        IndexIterator indexIterator = index.indexedIterator(streamPrefix, streams::matchStreamHash, Checkpoint.of(longs), ordered);

        IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
        return createEventLogIterator(this::tryGetMetadata, indexedLogIterator);
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streamNames, boolean ordered) {
        if (streamNames.size() == 1) {
            return fromStream(streamNames.iterator().next());
        }

//        final Map<String, StreamMetadata> staticMetadata = streamNames.stream()
//                .map(StreamName::hash)
//                .map(this::fetchMetadata)
//                .collect(Collectors.toMap(StreamMetadata::name, Function.identity()));

        Set<Long> hashes = streamNames.stream().map(StreamName::hash).collect(Collectors.toSet());


        IndexIterator indexIterator = index.indexedIterator(Checkpoint.of(hashes), ordered);
        IndexedLogIterator indexedLogIterator = new IndexedLogIterator(indexIterator, eventLog);
        return createEventLogIterator(this::tryGetMetadata, indexedLogIterator);
    }

    @Override
    public int version(String stream) {
        long streamHash = streams.hashOf(stream);
        return streams.version(streamHash);
    }

    @Override
    public int count(String stream) {
        return streams.get(stream).map(m -> {
            int version = version(stream);
            return m.truncated > NO_TRUNCATE ? version - m.truncated : version + 1;
        }).orElse(0);
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        LogIterator<EventRecord> logIterator = eventLog.iterator(Direction.FORWARD);
        NonIndexedLogIterator nonIndexedLogIterator = new NonIndexedLogIterator(logIterator);
        EventPolicyFilterIterator eventPolicyFilterIterator = new EventPolicyFilterIterator(nonIndexedLogIterator, linkToPolicy, systemEventPolicy);
        if (LinkToPolicy.RESOLVE.equals(linkToPolicy)) {
            return new LinkToResolveIterator(eventPolicyFilterIterator, this::resolve);
        }
        return eventPolicyFilterIterator;
    }

    @Override
    public EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        requireNonNull(lastEvent, "last event must be provided");
        Optional<IndexEntry> indexEntry = index.get(lastEvent.hash(), lastEvent.version());
        IndexEntry entry = indexEntry.orElseThrow(() -> new IllegalArgumentException("No index entry found for " + lastEvent));
        LogIterator<EventRecord> logIterator = eventLog.iterator(Direction.FORWARD, entry.position);
        NonIndexedLogIterator nonIndexedLogIterator = new NonIndexedLogIterator(logIterator);
        EventPolicyFilterIterator eventPolicyFilterIterator = new EventPolicyFilterIterator(nonIndexedLogIterator, linkToPolicy, systemEventPolicy);
        if (LinkToPolicy.RESOLVE.equals(linkToPolicy)) {
            return new LinkToResolveIterator(eventPolicyFilterIterator, this::resolve);
        }
        return eventPolicyFilterIterator;
    }

    @Override
    public EventRecord linkTo(String stream, final EventRecord event) {
        Future<EventRecord> future = eventWriter.queue(writer -> {
            EventRecord resolved = resolve(event);
            StreamMetadata metadata = getOrCreateStream(writer, stream);
            EventRecord linkTo = LinkTo.create(stream, StreamName.from(resolved));
            return writer.append(linkTo, NO_EXPECTED_VERSION, metadata); // TODO expected version for LinkTo
        });

        return Threads.waitFor(future);
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
            return writer.append(linkTo, NO_EXPECTED_VERSION, metadata);
        });
        return Threads.waitFor(future);
    }

    private StreamMetadata getOrCreateStream(Writer writer, String stream) {
        return streams.createIfAbsent(stream, created -> {
            EventRecord eventRecord = StreamCreated.create(created);
            StreamMetadata metadata = streams.get(SystemStreams.STREAMS).get();
            writer.append(eventRecord, NO_EXPECTED_VERSION, metadata);
        });
    }

    @Override
    public EventRecord get(StreamName stream) {
        EventRecord record = getInternal(stream);
        return resolve(record);
    }

    //GET WITHOUT RESOLVING
    private EventRecord getInternal(StreamName stream) {
        if (!stream.hasVersion()) {
            throw new IllegalArgumentException("Version must be greater than " + NO_VERSION);
        }
        Optional<IndexEntry> indexEntry = index.get(stream.hash(), stream.version());
        if (!indexEntry.isPresent()) {
            return null;
        }

        IndexEntry ie = indexEntry.get();
        return eventLog.get(ie.position);
    }

    private EventRecord resolve(EventRecord record) {
        if (record == null) {
            return null;
        }
        if (record.isLinkToEvent()) {
            LinkTo linkTo = LinkTo.from(record);
            StreamName targetEvent = StreamName.of(linkTo.stream, linkTo.version);
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
        StringUtils.requireNonBlank(event.stream, "closeableStream must be provided");
        StringUtils.requireNonBlank(event.type, "Type must be provided");
        if (event.stream.startsWith(StreamName.SYSTEM_PREFIX)) {
            throw new IllegalArgumentException("Stream cannot start with " + StreamName.SYSTEM_PREFIX);
        }
        if (LinkTo.TYPE.equals(event.type)) {
            throw new IllegalArgumentException("Stream type cannot be " + LinkTo.TYPE);
        }
    }

    private void onIndexFlushed(TableIndex.FlushInfo flushInfo) {
        eventWriter.queue(writer -> {
            var indexFlushedEvent = IndexFlushed.create(flushInfo.logPosition, flushInfo.timeTaken, flushInfo.entries);
            StreamMetadata metadata = getOrCreateStream(writer, indexFlushedEvent.stream);
            writer.append(indexFlushedEvent, NO_EXPECTED_VERSION, metadata);
        });
    }

    private EventLogIterator createEventLogIterator(Function<String, StreamMetadata> metadataSupplier, IndexedLogIterator indexedLogIterator) {
        MaxAgeFilteringIterator maxAgeFilteringIterator = new MaxAgeFilteringIterator(metadataSupplier, indexedLogIterator);
        return new LinkToResolveIterator(maxAgeFilteringIterator, this::resolve);
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