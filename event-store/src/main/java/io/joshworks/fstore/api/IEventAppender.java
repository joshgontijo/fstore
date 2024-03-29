package io.joshworks.fstore.api;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;

public interface IEventAppender {

    EventRecord linkTo(String stream, EventRecord event);

    //TODO this can be dangerous, ideally it would be internal, but linkTo on projections can benefit from avoiding the event parsing
    EventRecord linkTo(String dstStream, EventId source, String sourceType);

    EventRecord append(EventRecord event);

    EventRecord append(EventRecord event, int expectedVersion);

}
