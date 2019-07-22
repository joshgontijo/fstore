package io.joshworks.fstore.serializer;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class CharacterSerializer implements Serializer<Character> {

    @Override
    public void writeTo(Character data, ByteBuffer dst) {
        dst.putChar(data);
    }

    @Override
    public Character fromBytes(ByteBuffer data) {
        return data.getChar();
    }
}
