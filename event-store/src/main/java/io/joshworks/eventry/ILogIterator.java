package io.joshworks.eventry;

import java.util.Set;

public interface ILogIterator {

    StreamIterator fromStream(StreamName stream);

    StreamIterator fromStreams(String streamPattern);

    StreamIterator fromStreams(Set<StreamName> streams);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventLogIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
