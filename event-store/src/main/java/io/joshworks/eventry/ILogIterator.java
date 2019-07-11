package io.joshworks.eventry;

import java.util.Set;

public interface ILogIterator {

    EventLogIterator fromStream(StreamName stream);

    EventLogIterator fromStreams(String streamPattern);

    EventLogIterator fromStreams(Set<StreamName> streams);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
