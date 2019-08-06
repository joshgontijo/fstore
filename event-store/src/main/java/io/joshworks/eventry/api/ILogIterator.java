package io.joshworks.eventry.api;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.SystemEventPolicy;

public interface ILogIterator {

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent);

}
