package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;

public interface IStreamQuery {

    EventRecord get(StreamName stream);

    int version(String stream);

    int count(String stream);

}
