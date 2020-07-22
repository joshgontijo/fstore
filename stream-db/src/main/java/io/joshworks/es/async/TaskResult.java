package io.joshworks.es.async;

public record TaskResult(
        long logAddress,
        int size,
        long stream,
        int version,
        long sequence) {

}
