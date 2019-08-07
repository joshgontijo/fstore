package io.joshworks.fstore.es.shared;

public enum Status {
    ACTIVE, // Can accept requests
    INACTIVE, // Not yet initialized
    UNAVAILABLE // Cannot accept requests
}