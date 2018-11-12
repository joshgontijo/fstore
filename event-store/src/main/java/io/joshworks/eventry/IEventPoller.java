package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogPoller;

import java.util.Map;
import java.util.Set;

public interface IEventPoller {

    LogPoller<EventRecord> logPoller();

    LogPoller<EventRecord> logPoller(long position);

    LogPoller<EventRecord> streamPoller(Set<String> streams);

    LogPoller<EventRecord> streamPoller(Map<String, Integer> streams);

}
