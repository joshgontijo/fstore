package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.util.Set;

public interface ILogIterator {

    StreamIterator fromStream(StreamName stream);

    StreamIterator fromStreams(String streamPattern);

    StreamIterator fromStreams(Set<StreamName> streams);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
