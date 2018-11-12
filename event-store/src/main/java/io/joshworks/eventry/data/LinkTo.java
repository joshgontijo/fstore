package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;

public class LinkTo {

    public static final String TYPE = Constant.SYSTEM_PREFIX + ">";

    public final String stream;
    public final int version;

    public LinkTo(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    public static EventRecord create(String stream, StreamFormat streamFormat) {
        return EventRecord.create(stream, TYPE, StringUtils.toUtf8Bytes(streamFormat.toString()));
    }

    public static LinkTo from(EventRecord record) {
        String streamVersion = record.dataAsString();
        StreamFormat streamFormat = StreamFormat.parse(streamVersion);
        return new LinkTo(streamFormat.stream, streamFormat.version);
    }

}
