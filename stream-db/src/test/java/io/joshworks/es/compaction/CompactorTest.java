package io.joshworks.es.compaction;

import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.joshworks.es.EventHelper.evOf;
import static org.junit.Assert.assertEquals;

public class CompactorTest {

    private Log log;
    private File testFolder;

    @Before
    public void setUp() {
        testFolder = TestUtils.testFolder();
        log = open();
    }

    @After
    public void tearDown() {
        try {
            log.close();
            TestUtils.deleteRecursively(testFolder);
        } catch (Exception e) {
            System.err.println("Failed to delete folder " + testFolder.getName());
        }
    }

    public Log open() {
        return new Log(testFolder, Size.MB.ofInt(1));
    }

    @Test
    public void compact() {
        String stream = "stream-1";
        String oldPrefix = "old-event-";
        String newPrefix = "new-event-";

        for (int i = 0; i < 10000000; i++) {
            log.append(evOf(123, stream, i, oldPrefix + i));
        }
        log.roll();

        for (int i = 0; i < 10000000; i++) {
            log.append(evOf(123, stream, i, newPrefix + i));
        }
        log.roll();

        Compactor compactor = new Compactor();
        compactor.compactLog(log, 2);

        System.out.println(log.segmentsNames());

        assertEquals(2, log.segments());
        assertEquals(1, log.depth());

    }


    private void append(String stream, int version, String value) {

    }

}