package io.joshworks.eventry.stream;

import java.util.Map;

public class StreamMetadata {

    //not using enums for easier serialization
    public static final int PERMISSION_NONE = 0;
    public static final int PERMISSION_READ = 1;
    public static final int PERMISSION_WRITE = 2;

    public static final int STREAM_ACTIVE = 0;
    public static final int STREAM_LOCKED = 1;
    public static final int STREAM_DELETED = 2;

    public static final int NO_MAX_AGE = -1;
    public static final int NO_MAX_COUNT = -1;

    public static final int NO_TRUNCATE = 0;


    public final String name;
    public final long hash;
    public final long created;

    public final long maxAge;
    public final int maxCount;

    public final int truncated;

    public final int state;

    //TODO lot of memory usage when creating many streams
    final Map<String, Integer> acl;
    final Map<String, String> metadata;

    public StreamMetadata(String name, long hash, long created, long maxAge, int maxCount, int truncated, Map<String, Integer> acl, Map<String, String> metadata, int state) {
        this.name = name;
        this.hash = hash;
        this.created = created;
        this.maxAge = maxAge;
        this.maxCount = maxCount;
        this.truncated = truncated;
        this.acl = acl;
        this.metadata = metadata;
        this.state = state;
    }


    public boolean hasReadPermission(String id) {
        return acl.getOrDefault(id, PERMISSION_NONE).equals(PERMISSION_READ);
    }

    public boolean hasWritePermission(String id) {
        return acl.getOrDefault(id, PERMISSION_NONE).equals(PERMISSION_WRITE);
    }

    public boolean streamDeleted() {
        return state == STREAM_DELETED;
    }

    public boolean streamActive() {
        return state == STREAM_ACTIVE;
    }

    public boolean streamLocked() {
        return state == STREAM_LOCKED;
    }

    public boolean truncated() {
        return truncated > 0;
    }

    public String metadata(String key) {
        return metadata.get(key);
    }

    public String name() {
        return name;
    }


    @Override
    public String toString() {
        return "StreamMetadata{" +
                "name='" + name + '\'' +
                ", hash=" + hash +
                ", created=" + created +
                ", maxAge=" + maxAge +
                ", truncateBefore=" + truncated +
                ", maxCount=" + maxCount +
                ", state=" + state +
                ", acl=" + acl +
                ", metadata=" + metadata +
                '}';
    }
}

