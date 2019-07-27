package io.joshworks.eventry;

import io.joshworks.eventry.stream.StreamMetadata;

import java.util.function.Function;

public class EventUtils {

    public static boolean validIndexEntry(StreamMetadata metadata, int version, long timestamp, Function<Long, Integer> versionFetcher) {
        if (metadata.streamDeleted()) {
            return false;
        }
        if (isExpired(timestamp, metadata)) {
            return false;
        }
        if (isTruncated(version, metadata.truncated)) {
            return false;
        }
        if (metadata.maxCount > StreamMetadata.NO_MAX_COUNT) {
            int streamVersion = versionFetcher.apply(metadata.hash);
            return !isObsolete(version, metadata, streamVersion);
        }
        return true;
    }

    private static boolean isTruncated(int version, int truncatedVersion) {
        return version <= truncatedVersion;
    }

    private static boolean isObsolete(int version, StreamMetadata metadata, int streamVersion) {
        return metadata.maxCount > 0 && streamVersion - version >= metadata.maxCount;
    }

    private static boolean isExpired(long recordTimestamp, StreamMetadata metadata) {
        return metadata.maxAgeSec > 0 && System.currentTimeMillis() - recordTimestamp > metadata.maxAgeSec;
    }
}
