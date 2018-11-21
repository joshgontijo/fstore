package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.util.Set;

public interface ILogIterator {

    //TODO remove me ?? just used for log dump
    LogIterator<IndexEntry> scanIndex();

    EventLogIterator fromStream(StreamName stream);

    EventLogIterator fromStreams(String streamPattern);

    EventLogIterator fromStreams(Set<StreamName> streams);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
