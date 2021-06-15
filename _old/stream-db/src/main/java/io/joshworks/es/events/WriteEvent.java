package io.joshworks.es.events;

public record WriteEvent(String stream, String type, int expectedVersion, byte[] data) {

}
