package io.joshworks.eventry;

import io.joshworks.eventry.stream.StreamMetadata;

import java.util.function.Function;

public class EventUtils {

    public static boolean isTruncated(int version, StreamMetadata metadata) {
        return metadata.truncated() && version <= metadata.truncated;
    }

    public static boolean isObsolete(int version, StreamMetadata metadata, int streamVersion) {
        return metadata.maxCount > 0 && streamVersion - version >= metadata.maxCount;
    }

    public static boolean isExpired(long recordTimestamp, StreamMetadata metadata) {
        return metadata.maxAge > 0 && System.currentTimeMillis() - recordTimestamp > metadata.maxAge;
    }

    public static boolean skipEntry(StreamMetadata metadata, int version, long timestamp, Function<Long, Integer> versionFetcher) {
        if (metadata.streamDeleted()) {
            return true;
        }
        if (isExpired(timestamp, metadata)) {
            return true;
        }
        if (isTruncated(version, metadata)) {
            return true;
        }
        if (metadata.maxCount > StreamMetadata.NO_MAX_COUNT) {
            int streamVersion = versionFetcher.apply(metadata.hash);
            return isObsolete(version, metadata, streamVersion);
        }
        return false;
    }

}
