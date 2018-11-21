package io.joshworks.eventry;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;

public interface IStreamQuery {

    EventRecord get(StreamName stream);

    EventRecord get(IndexEntry entry);

    EventRecord resolve(EventRecord record);

    int version(String stream);

}
