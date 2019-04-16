package io.joshworks.eventry.server.cluster.partition;

public enum Status {
    ACTIVE, // Can accept requests
    INACTIVE, // Not yet initialized
    UNAVAILABLE // Cannot accept requests
}
