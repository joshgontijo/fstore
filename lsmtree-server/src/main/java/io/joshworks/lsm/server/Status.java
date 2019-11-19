package io.joshworks.lsm.server;

public enum Status {
    ACTIVE, // Can accept requests
    INACTIVE, // Not yet initialized
    UNAVAILABLE // Cannot accept requests
}