package io.joshworks.fstore.log.segment.header;

public class InvalidMagic extends HeaderException {

    public InvalidMagic(long expected, long actual) {
        super("Invalid magic: Expected: '" + expected + "', actual: '" + actual + "'");
    }
}
