package io.joshworks.ilog.fields;

import io.joshworks.fstore.core.io.buffers.Buffers;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ArrayFieldTest {

    @Test
    public void readValue() {
        long value = 12345L;
        int valueSize = Long.BYTES;
        int items = 10;

        int expectedFieldSize = valueSize + ArrayField.HEADER_BYTES;

        ByteBuffer fieldBuffer = Buffers.allocate(100, false);
        ByteBuffer valueBuffer = Buffers.allocate(valueSize, false).putLong(value).flip();

        ArrayField field = new ArrayField(0, valueSize);

        for (int i = 0; i < items; i++) {
            field.add(fieldBuffer, valueBuffer, Long.BYTES);
        }


        assertEquals(expectedFieldSize, field.len(fieldBuffer));
        assertEquals(valueSize, field.valueLen());

        ByteBuffer dst = Buffers.allocate(valueSize, false);
        int read = field.copyValueTo(fieldBuffer, dst);
        assertEquals(valueSize, read);
        assertEquals(value, dst.flip().getLong());
    }
}