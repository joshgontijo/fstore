package io.joshworks.eventry;

import java.util.Set;

public interface ILogIterator {

    EventLogIterator fromStream(StreamName stream);

    EventLogIterator fromStreams(String streamPattern, boolean ordered);

    EventLogIterator fromStreams(Set<StreamName> streams, boolean ordered);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
