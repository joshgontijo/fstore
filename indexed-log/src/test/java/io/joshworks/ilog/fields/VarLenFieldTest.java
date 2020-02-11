package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class VarLenFieldTest {

    @Test
    public void readValue() {
        long value = 12345L;
        int valueSize = Long.BYTES;
        int expectedFieldSize = valueSize + VarLenField.HEADER_BYTES;

        ByteBuffer fieldBuffer = Buffers.allocate(100, false);
        ByteBuffer valueBuffer = Buffers.allocate(valueSize, false).putLong(value).flip();

        VarLenField field = new VarLenField(0);
        field.set(fieldBuffer, valueBuffer);

        assertEquals(expectedFieldSize, field.len(fieldBuffer));
        assertEquals(valueSize, field.valueLen(fieldBuffer));

        ByteBuffer dst = Buffers.allocate(valueSize, false);
        int read = field.copyValueTo(fieldBuffer, dst);
        assertEquals(valueSize, read);
        assertEquals(value, dst.flip().getLong());
    }
}