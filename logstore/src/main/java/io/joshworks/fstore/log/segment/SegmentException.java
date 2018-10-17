package io.joshworks.fstore.log.segment;

public class SegmentException extends RuntimeException {

    public SegmentException(String message) {
        super(message);
    }

    public SegmentException(String message, Throwable cause) {
        super(message, cause);
    }

    public SegmentException(Throwable cause) {
        super(cause);
    }
}
