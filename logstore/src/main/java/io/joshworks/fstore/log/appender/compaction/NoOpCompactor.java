package io.joshworks.fstore.log.appender.compaction;

import java.io.IOException;

public class NoOpCompactor implements ICompactor {

    @Override
    public void compact() {
        //do nothing
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }
}
