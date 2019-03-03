package io.joshworks.eventry.projection;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projection.result.ScriptExecutionResult;
import io.joshworks.eventry.projection.task.ProjectionContext;
import io.joshworks.fstore.core.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Jsr223HandlerTest {

    private static final String ENGINE_NAME = "nashorn";
//    private IEventStore store;
//    private File testFile;

    @Before
    public void setUp() throws Exception {
//        testFile = FileUtils.testFolder();
//        store = EventStore.open(testFile);
    }

    @Test
    public void linkTo_returns_the_linked_events_in_the_output() throws ScriptExecutionException {
        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("test-script.js"));
        ProjectionContext dummyContext = new ProjectionContext(null);
        Jsr223Handler handler = new Jsr223Handler(dummyContext, script, ENGINE_NAME);

        var ev1 = EventRecord.create("stream1", "type-1", Map.of("age", 1));
        var ev2 = EventRecord.create("stream1", "type-1", Map.of("age", 2));
        var ev3 = EventRecord.create("stream1", "type-1", Map.of("age", 3));

        var events = List.of(ev1, ev2, ev3);

        State state = new State();
        ScriptExecutionResult result = handler.processEvents(events, state);

        assertEquals(3, result.linkToEvents);
        assertEquals(3, result.outputEvents.size());
    }

    @Test
    public void state_is_returned_correctly() throws ScriptExecutionException {
        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("state.js"));
        ProjectionContext dummyContext = new ProjectionContext(null);
        Jsr223Handler handler = new Jsr223Handler(dummyContext, script, ENGINE_NAME);

        var ev1 = EventRecord.create("stream1", "type-1", Map.of("age", 1));
        var ev2 = EventRecord.create("stream1", "type-1", Map.of("age", 2));
        var ev3 = EventRecord.create("stream1", "type-1", Map.of("age", 3));

        var events = List.of(ev1, ev2, ev3);

        ScriptExecutionResult result = handler.processEvents(events, dummyContext.state());

        assertEquals(13, dummyContext.state().get("count"));
    }

    @Test
    public void script_error() {
        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("error.js"));
        ProjectionContext dummyContext = new ProjectionContext(null);
        Jsr223Handler handler = new Jsr223Handler(dummyContext, script, ENGINE_NAME);

        var ev1 = EventRecord.create("stream1", "type-1", Map.of("age", 1));
        var ev2 = EventRecord.create("stream1", "type-1", Map.of("age", 2));
        var ev3 = EventRecord.create("stream1", "type-1", Map.of("age", 3));

        var events = List.of(ev1, ev2, ev3);

        State state = new State();
        try {
            ScriptExecutionResult result = handler.processEvents(events, state);
            System.out.println(result);
        } catch (ScriptExecutionException e) {
            System.out.println(e);
        }
    }
}