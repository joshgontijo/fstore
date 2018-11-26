package io.joshworks.fstore.log.appender;

public enum FlushMode {
    PERIODICALLY(0),
    MANUAL(1),
    ON_ROLL(2),
    ALWAYS(3);

    public final int code;

    FlushMode(int code) {
        this.code = code;
    }

    static FlushMode of(int type) {
        for (FlushMode theType : FlushMode.values()) {
            if (theType.code == type) {
                return theType;
            }
        }
        throw new IllegalArgumentException("Invalid FlushMode value: " + type);

    }
}
