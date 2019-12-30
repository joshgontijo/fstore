package io.joshworks.fstore.log.appender.naming;

import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class SequentialNamingTest {

    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = TestUtils.testFolder();
        testDirectory.deleteOnExit();
    }

    @After
    public void cleanup() {
        TestUtils.deleteRecursively(testDirectory);
    }

    @Test
    public void index_is_restore_when_reopening() throws IOException {
        SequentialNaming sequentialNaming = new SequentialNaming(testDirectory);

        int numFiles = 10;

        for (int i = 0; i < numFiles; i++) {
            String name = sequentialNaming.prefix() + ".log";
            Files.createFile(new File(testDirectory, name).toPath());
        }

        sequentialNaming = new SequentialNaming(testDirectory);
        assertEquals(numFiles, sequentialNaming.counter.get());

    }
}