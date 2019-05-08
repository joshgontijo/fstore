package io.joshworks.eventry.partition;

public enum Status {
    ACTIVE, // Can accept requests
    INACTIVE, // Not yet initialized
    UNAVAILABLE // Cannot accept requests
}
