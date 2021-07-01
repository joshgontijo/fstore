package io.joshworks.es2.log;

import java.nio.ByteBuffer;

import static io.joshworks.es2.log.TLog.TYPE_OFFSET;

enum Type {
    DATA((byte) 1),
    FLUSH((byte) 2);

    final byte i;

    Type(byte i) {
        this.i = i;
    }

    static Type of(ByteBuffer data) {
        byte b = data.get(data.position() + TYPE_OFFSET);
        return of(b);
    }

    static Type of(byte b) {
        return switch (b) {
            case 1 -> DATA;
            case 2 -> FLUSH;
            default -> null;
        };
    }
}
