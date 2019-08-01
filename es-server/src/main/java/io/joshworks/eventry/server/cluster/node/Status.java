package io.joshworks.eventry.server.cluster.node;

public enum Status {
    ACTIVE, // Can accept requests
    INACTIVE, // Not yet initialized
    UNAVAILABLE // Cannot accept requests
}
