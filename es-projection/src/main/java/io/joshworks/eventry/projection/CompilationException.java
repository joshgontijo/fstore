package io.joshworks.eventry.projection;

public class CompilationException extends ProjectionException {
    CompilationException(String message, Exception e) {
        super(message, e);
    }
}
