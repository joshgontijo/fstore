package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;

public interface IEventAppender {

    EventRecord linkTo(String stream, EventRecord event);

    //TODO this can be dangerous, ideally it would be internal, but linkTo on projections can benefit from avoiding the event parsing
    EventRecord linkTo(String dstStream, String sourceStream, int sourceVersion, String sourceType);

    EventRecord append(EventRecord event);

    EventRecord append(EventRecord event, int expectedVersion);

}
