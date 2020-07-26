package io.joshworks.es.events;

public record LinkToEvent(String srcStream, int srcVersion, String dstStream, int expectedVersion) {
}
