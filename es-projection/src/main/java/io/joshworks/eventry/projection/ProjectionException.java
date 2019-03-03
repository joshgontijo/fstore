package io.joshworks.eventry.projection;

public class ProjectionException extends RuntimeException {
    ProjectionException(String message) {
        super(message);
    }

    ProjectionException(String message, Throwable cause) {
        super(message, cause);
    }

    ProjectionException(Throwable cause) {
        super(cause);
    }

}
