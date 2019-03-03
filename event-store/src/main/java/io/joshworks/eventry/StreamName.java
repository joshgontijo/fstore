package io.joshworks.eventry;

import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;

import static io.joshworks.eventry.index.IndexEntry.NO_VERSION;

public class StreamName {

    public static final String SYSTEM_PREFIX = "_";
    public static final String STREAM_VERSION_SEPARATOR = "@";
    private static final StreamHasher hasher = new StreamHasher(new XXHash(), new Murmur3Hash());

    private final String name;
    private final int version;

    private StreamName(String name, int version) {
        this.name = name;
        this.version = version < NO_VERSION ? NO_VERSION : version;
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    public long hash() {
        return hasher.hash(name);
    }

    public boolean isSystemStream() {
        return name.startsWith(SYSTEM_PREFIX);
    }

    public boolean isAll() {
        return SystemStreams.ALL.equals(name);
    }

    public boolean hasVersion() {
        return version > NO_VERSION;
    }

    public static StreamName of(String stream, int version) {
        StringUtils.requireNonBlank(stream);
        StreamName parsed = parse(stream);
        version = version <= NO_VERSION ? NO_VERSION : version;
        return new StreamName(parsed.name, version);
    }

    public static StreamName parse(String streamVersion) {
        if (StringUtils.isBlank(streamVersion)) {
            throw new IllegalArgumentException("Null or empty stream value");
        }
        String[] split = streamVersion.split(STREAM_VERSION_SEPARATOR, 2);
        if (!(split.length >= 1 && split.length < 3)) {
            throw new IllegalArgumentException("Invalid stream format: '" + streamVersion + "'");
        }

        String name = validateStreamName(split, streamVersion);
        int version = getVersion(split, streamVersion);
        return new StreamName(name, version);
    }

    public static StreamName from(EventRecord eventRecord) {
        return new StreamName(eventRecord.stream, eventRecord.version);
    }

    public static long hash(String streamName) {
        return hasher.hash(streamName);
    }


    private static String validateStreamName(String[] split, String original) {
        String streamName = split[0];
        if (StringUtils.isBlank(streamName)) {
            throw new IllegalArgumentException("Null or empty stream name: '" + original + "'");
        }
        if (streamName.contains("@")) {
            throw new IllegalArgumentException("Invalid stream name format: '" + original + "'");
        }
        return streamName;
    }

    private static int getVersion(String[] split, String original) {
        if (split.length <= 1) {
            return NO_VERSION;
        }
        try {
            return Integer.parseInt(StringUtils.requireNonBlank(split[1], "Stream version"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version for '" + original + "': ", e);
        }
    }

    public static String toString(String stream, int version) {
        if (version <= NO_VERSION) {
            return stream;
        }
        return stream + STREAM_VERSION_SEPARATOR + version;
    }

    @Override
    public String toString() {
        return toString(name, version);
    }

}
