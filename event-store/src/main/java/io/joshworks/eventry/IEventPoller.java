package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.Set;

public interface IEventPoller {

    PollingSubscriber<EventRecord> poller();

    PollingSubscriber<EventRecord> poller(long position);

    PollingSubscriber<EventRecord> poller(String stream);

    PollingSubscriber<EventRecord> poller(Set<String> streamNames);

}
