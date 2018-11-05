package io.joshworks.eventry.projections;

public class CompilationException extends ProjectionException {
    CompilationException(String message, Exception e) {
        super(message, e);
    }
}
