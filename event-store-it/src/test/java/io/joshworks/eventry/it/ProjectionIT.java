package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProjectionIT {

    private File directory;
    private EventStore store;

    @Before
    public void setUp() {
        directory = FileUtils.testFolder();
        store = EventStore.open(directory);
    }

    @After
    public void tearDown() {
        store.close();
        FileUtils.tryDelete(directory);
    }

    @Test
    public void create() {
        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("by-value.js"));

        //values from file
        Projection projection = store.createProjection(script);
        assertEquals("by-value", projection.name);
        assertEquals(10, projection.batchSize);
        assertEquals(Projection.Type.ONE_TIME, projection.type);
        assertFalse(projection.parallel);
        assertEquals(Set.of("test-stream"), projection.sources);
        assertTrue(projection.enabled);
    }


    @Test
    public void state() throws InterruptedException {

        final int size = 1000000;
        String stream = "test-stream"; //from file
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, Map.of("value", i)));
        }

        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("state-inc-value.js"));
        Projection projection = store.createProjection(script);

        store.runProjection(projection.name);

        Thread.sleep(60000);

//        State state = store.projectionState(projection.name);
//        Number evCounter = (Number) state.get("evCounter");
//        Number valueSum = (Number) state.get("valueSum");
//        System.out.println(valueSum.longValue());
//        System.out.println(evCounter.longValue());
//
//
//        store.close();
//
//        store = EventStore.open(directory);
//        state = store.projectionState(projection.name);
//        evCounter = (Number) state.get("evCounter");
//        valueSum = (Number) state.get("valueSum");
//        System.out.println(valueSum.longValue());
//        System.out.println(evCounter.longValue());


    }

    @Test
    public void onStart() throws InterruptedException {

        final int size = 1000000;
        String stream = "test-stream"; //from file
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, Map.of("value", i)));
        }

        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("on-start.js"));
        Projection projection = store.createProjection(script);

        store.runProjection(projection.name);

        Thread.sleep(30000);

        EventRecord eventRecord = store.get(StreamName.of("my-state", 0));
        assertNotNull(eventRecord);
        JsonEvent jsonEvent = JsonEvent.from(eventRecord);
        assertTrue((Boolean) jsonEvent.body.get("started"));


//


    }

    @Test
    public void publish_state() throws InterruptedException {

        final int size = 1000000;
        String stream = "test-stream"; //from file
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, Map.of("value", i)));
        }

        String script = IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream("publish-state.js"));
        Projection projection = store.createProjection(script);

        store.runProjection(projection.name);

        Thread.sleep(30000);

        //projection name  + -state
        store.fromStream(StreamName.of("my-state"))
                .stream()
                .map(JsonEvent::from)
                .forEach(System.out::println);


    }

}
