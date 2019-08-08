package io.joshworks.eventry.stream;

import java.util.Map;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static java.util.Objects.requireNonNullElse;

public class StreamInfo {

    public final String name;
    public final long hash;
    public final long created;

    public final int version;
    public final int maxAge;
    public final int maxCount;

    public final Map<String, Integer> permissions;
    public final Map<String, String> metadata;

    private StreamInfo(String name, long hash, long created, Integer maxAge, Integer maxCount, int version, Map<String, Integer> permissions, Map<String, String> metadata) {
        this.name = name;
        this.hash = hash;
        this.created = created;
        this.maxAge = requireNonNullElse(maxAge, NO_MAX_AGE);
        this.maxCount = requireNonNullElse(maxCount, NO_MAX_COUNT);
        this.version = version;
        this.permissions = permissions;
        this.metadata = metadata;
    }

    public static StreamInfo from(StreamMetadata metadata, int version) {
        return new StreamInfo(metadata.name, metadata.hash, metadata.created, metadata.maxAgeSec, metadata.maxCount, version, metadata.acl, metadata.metadata);
    }

}

