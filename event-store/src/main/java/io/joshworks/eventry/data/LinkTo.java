package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;

public class LinkTo {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + ">";

    public final String stream;
    public final int version;

    private LinkTo(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static EventRecord create(String stream, StreamName streamName) {
        return EventRecord.create(stream, TYPE, StringUtils.toUtf8Bytes(streamName.toString()));
    }

    public static LinkTo from(EventRecord record) {
        String streamVersion = record.dataAsString();
        StreamName streamName = StreamName.parse(streamVersion);
        return new LinkTo(streamName.name(), streamName.version());
    }

}
