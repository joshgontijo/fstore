package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;

import static io.joshworks.eventry.data.Constant.STREAM_VERSION_SEPARATOR;

public class StreamFormat {
    public final String stream;
    public final int version;

    private StreamFormat(String stream, int version) {
        this.stream = stream;
        this.version = version;
    }

    @Override
    public String toString() {
        return toString(stream, version);
    }

    public static StreamFormat of(String stream, int version) {
        StringUtils.requireNonBlank(stream);
        return new StreamFormat(stream, version);
    }

    public static StreamFormat of(EventRecord eventRecord) {
        return new StreamFormat(eventRecord.stream, eventRecord.version);
    }

    public static StreamFormat parse(String streamVersion) {
        if (StringUtils.isBlank(streamVersion)) {
            throw new IllegalArgumentException("Invalid stream value");
        }
        String[] split = streamVersion.split(STREAM_VERSION_SEPARATOR);
        if (split.length == 1) {
            return new StreamFormat(split[0], 0);
        } else if (split.length == 2) {
            String streamName = StringUtils.requireNonBlank(split[0], "Stream name");
            int version = Integer.parseInt(StringUtils.requireNonBlank(split[1], "Stream version"));
            return new StreamFormat(streamName, version);
        } else {
            throw new IllegalArgumentException("Invalid stream name format: '" + streamVersion + "'");
        }
    }

    public static String toString(String stream, int version) {
        return stream + STREAM_VERSION_SEPARATOR + version;
    }

}
