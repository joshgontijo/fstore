package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface IStreamQuery {

    EventRecord get(String stream, int version);

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
}
