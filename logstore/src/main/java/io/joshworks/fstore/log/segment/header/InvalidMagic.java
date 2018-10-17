package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.log.segment.SegmentException;

public class InvalidMagic extends SegmentException {

    public InvalidMagic(String expected, String actual) {
        super("Invalid magic: Expected: '" + expected + "', actual: '" + actual + "'");
    }
}
