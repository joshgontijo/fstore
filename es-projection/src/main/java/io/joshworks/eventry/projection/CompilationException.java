package io.joshworks.fstore.projection;

public class CompilationException extends ProjectionException {
    CompilationException(String message, Exception e) {
        super(message, e);
    }
}
