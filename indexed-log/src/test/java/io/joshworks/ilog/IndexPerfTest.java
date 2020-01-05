package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;

public class IndexPerfTest {

    private Index index;
    private File testFile;

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        index = new LongIndex(testFile, Size.GB.ofInt(1));
    }

    @After
    public void tearDown() throws Exception {
        index.delete();
    }
}
