package io.joshworks.eventry.data;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;

import java.nio.charset.StandardCharsets;

public class LinkTo {

    public static final String TYPE = ">";

    public final String stream;
    public final int version;

    private LinkTo(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static EventRecord create(String stream, EventId tgtEventId) {
        return EventRecord.create(stream, TYPE, StringUtils.toUtf8Bytes(tgtEventId.toString()));
    }

    public static LinkTo from(EventRecord record) {
        if(!record.isLinkToEvent()) {
            throw new IllegalArgumentException("Not a LinkTo type event");
        }
        String target = new String(record.body, StandardCharsets.UTF_8);
        EventId eventId = EventId.parse(target);
        return new LinkTo(eventId.name(), eventId.version());
    }

}
