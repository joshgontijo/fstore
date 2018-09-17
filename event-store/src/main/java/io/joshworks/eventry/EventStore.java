package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public interface EventStore {

    LogIterator<IndexEntry> keys();

    void cleanup();

    void createStream(String name);

    void createStream(String name, int maxCount, long maxAge);

    StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata);

    List<StreamInfo> streamsMetadata();

    Optional<StreamInfo> streamMetadata(String stream);

    LogIterator<EventRecord> fromStreamIter(String stream);

    Stream<EventRecord> fromStream(String stream);

    LogIterator<EventRecord> fromStreamIter(String stream, int versionInclusive);

    Stream<EventRecord> fromStream(String stream, int versionInclusive);

    Stream<EventRecord> zipStreams(Set<String> streams);

    LogIterator<EventRecord> zipStreamsIter(String stream);

    Stream<EventRecord> zipStreams(String streamPrefix);

    LogIterator<EventRecord> zipStreamsIter(Set<String> streamNames);

    Stream<Stream<EventRecord>> fromStreams(Set<String> streams);

    Map<String, Stream<EventRecord>> fromStreamsMapped(Set<String> streams);

    int version(String stream);

    LogIterator<EventRecord> fromAllIter();

    //Won't return the stream in the event !
    Stream<EventRecord> fromAll();

    EventRecord linkTo(String stream, EventRecord event);

    void emit(String stream, EventRecord event);

    EventRecord get(String stream, int version);

    //TODO make it price and change SingleStreamIterator
    EventRecord get(IndexEntry indexEntry);

    EventRecord append(EventRecord event);

    EventRecord append(EventRecord event, int expectedVersion);

    PollingSubscriber<EventRecord> poller();

    PollingSubscriber<EventRecord> poller(long position);

    PollingSubscriber<EventRecord> poller(String stream);

    PollingSubscriber<EventRecord> poller(Set<String> streamNames);

    long logPosition();

    void close();

    void add(EventRecord record);
}
