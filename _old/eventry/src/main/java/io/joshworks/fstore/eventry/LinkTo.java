package io.joshworks.fstore.eventry;

import java.nio.charset.StandardCharsets;

import static io.joshworks.fstore.eventry.utils.StringUtils.toUtf8Bytes;

public class LinkTo {

    public static final String TYPE = ">";

    public final String stream;
    public final int version;

    private LinkTo(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static EventRecord create(String stream, EventId tgtEventId) {
        return EventRecord.create(stream, TYPE, toUtf8Bytes(tgtEventId.toString()));
    }

    public static LinkTo from(EventRecord record) {
        if (!record.isLinkToEvent()) {
            throw new IllegalArgumentException("Not a LinkTo type event");
        }
        String target = new String(record.data, StandardCharsets.UTF_8);
        EventId eventId = EventId.parse(target);
        return new LinkTo(eventId.name(), eventId.version());
    }

}
