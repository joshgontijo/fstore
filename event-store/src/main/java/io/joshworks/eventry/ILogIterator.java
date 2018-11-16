package io.joshworks.eventry;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface ILogIterator {

    EventLogIterator fromStream(String stream);

    EventLogIterator fromStream(String stream, int versionInclusive);

    EventLogIterator zipStreams(String stream);

    EventLogIterator zipStreams(Set<String> streamNames);

    EventLogIterator fromAll();

    Stream<EventLogIterator> fromStreams(Set<String> streams);

    Map<String, EventLogIterator> fromStreamsMapped(Set<String> streams);

}
