package io.joshworks.es2;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventTest {


    @Test
    public void create() {

        var data = "value".getBytes(StandardCharsets.UTF_8);
        var event = Event.create(123, 222, "type_1", data);

        assertEquals(123, Event.stream(event));
        assertEquals(222, Event.version(event));
        assertEquals("type_1", Event.eventType(event));
        assertEquals("value", Event.dataString(event));

        assertTrue(Event.isValid(event));

    }
}