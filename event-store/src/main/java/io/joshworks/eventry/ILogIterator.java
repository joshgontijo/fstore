package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.util.Set;

public interface ILogIterator {

    EventLogIterator fromStream(StreamName stream);

    EventLogIterator fromStreams(String streamPattern, boolean ordered);

    EventLogIterator fromStreams(Set<StreamName> streams, boolean ordered);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
