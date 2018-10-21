package io.joshworks.fstore.log.segment;

public class SegmentClosedException extends RuntimeException {

    SegmentClosedException() {
    }

    SegmentClosedException(String s) {
        super(s);
    }
}
