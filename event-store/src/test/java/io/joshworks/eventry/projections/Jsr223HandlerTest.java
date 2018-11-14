package io.joshworks.eventry.projections;

import io.joshworks.eventry.ScriptExecutionException;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;
import io.joshworks.fstore.core.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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
    public void test_script() throws ScriptExecutionException {
        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("test-script.js"));
        ProjectionContext dummyContext = new ProjectionContext(null);
        Jsr223Handler handler = new Jsr223Handler(dummyContext, script, ENGINE_NAME);

        var ev1 = EventRecord.create("stream1", "type-1", Map.of("age", 1));
        var ev2 = EventRecord.create("stream1", "type-1", Map.of("age", 2));
        var ev3 = EventRecord.create("stream1", "type-1", Map.of("age", 3));

        var events = List.of(ev1, ev2, ev3);

        State state = new State();
        ScriptExecutionResult result = handler.processEvents(events, state);

        System.out.println(result);
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