package io.joshworks.eventry.api;

import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;

public interface ILogIterator {

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy);

    EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent);

}
