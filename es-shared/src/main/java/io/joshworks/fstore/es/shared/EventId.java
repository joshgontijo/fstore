package io.joshworks.fstore.es.shared;

import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.es.shared.utils.StringUtils;

import java.util.Objects;

public class EventId {

    //TODO ADD HEAD: stream-1@HEAD

    public static final String SYSTEM_PREFIX = "_";
    public static final String STREAM_VERSION_SEPARATOR = "@";
    private static final StreamHasher hasher = new StreamHasher(new XXHash(), new Murmur3Hash());

    public static final int START_VERSION = 0;
    public static final int MAX_VERSION = Integer.MAX_VALUE;
    public static final int NO_VERSION = -1;
    public static final int NO_EXPECTED_VERSION = -2;

    private final String stream;
    private final int version;

    private EventId(String stream, int version) {
        this.stream = stream;
        this.version = Math.max(version, NO_VERSION);
    }

    public String name() {
        return stream;
    }

    public int version() {
        return version;
    }

    public long hash() {
        return hasher.hash(stream);
    }

    public boolean isSystemStream() {
        return stream.startsWith(SYSTEM_PREFIX);
    }

    public boolean isAll() {
        return SystemStreams.ALL.equals(stream);
    }

    public boolean hasVersion() {
        return version > NO_VERSION;
    }

    public static EventId of(String stream) {
        return of(stream, NO_VERSION);
    }

    public static EventId of(String stream, int version) {
        StringUtils.requireNonBlank(stream);
        validateStreamName(stream);
        version = Math.max(version, NO_VERSION);
        return new EventId(stream, version);
    }

    public static EventId parse(String streamVersion) {
        if (StringUtils.isBlank(streamVersion)) {
            throw new IllegalArgumentException("Null or empty stream value");
        }
        String[] split = streamVersion.split(STREAM_VERSION_SEPARATOR, 2);
        if (!(split.length >= 1 && split.length < 3)) {
            throw new IllegalArgumentException("Invalid stream format: '" + streamVersion + "'");
        }

        validateStreamName(split[0]);
        int version = getVersion(split, streamVersion);
        return new EventId(split[0], version);
    }

    public static long hash(String streamName) {
        return hasher.hash(streamName);
    }


    private static void validateStreamName(String streamName) {
        if (StringUtils.isBlank(streamName)) {
            throw new IllegalArgumentException("Null or empty stream name: '" + streamName + "'");
        }
        if (streamName.contains(STREAM_VERSION_SEPARATOR)) {
            throw new IllegalArgumentException("Invalid stream name: '" + streamName + "'");
        }
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventId that = (EventId) o;
        return version == that.version &&
                Objects.equals(stream, that.stream);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, version);
    }

    @Override
    public String toString() {
        return toString(stream, version);
    }

}
