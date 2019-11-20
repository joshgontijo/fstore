package io.joshworks.fstore.api;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.LinkToPolicy;
import io.joshworks.fstore.SystemEventPolicy;

public interface ILogIterator {

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent);

}
