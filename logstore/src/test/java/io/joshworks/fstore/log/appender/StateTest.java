package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class StateTest {

    private File testFile;
    private State state;

    @Before
    public void setUp() {
        testFile = FileUtils.testFolder();
        state = State.empty(testFile);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(state);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void flush() {

        //give
        state.position(10);
        state.incrementEntryCount();
        state.lastRollTime(123L);

        state.flush();
        state.close();

        //when
        try(State found = State.readFrom(testFile)) {
            //then
            assertEquals(state.position(), found.position());
            assertEquals(state.entryCount(), found.entryCount());

        }


    }
}