package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.util.Set;

public interface ILogIterator {

    //TODO remove me ?? just used for log dump
    LogIterator<IndexEntry> scanIndex();

    EventLogIterator fromStream(String stream);

    EventLogIterator fromStream(String stream, int versionInclusive);

    EventLogIterator zipStreams(String stream);

    EventLogIterator zipStreams(Set<String> streamNames);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

}
