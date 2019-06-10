package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.history.data.HistoryItem;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;

import java.io.File;

public class History {

    private static final String DIRECTORY_NAME = "history";
    private final Segment<HistoryItem> segment;

    public History(File root, String magic, IDataStream dataStream) {
        File directory = new File(root, DIRECTORY_NAME);
        Storage storage = StorageProvider.of(StorageMode.RAF).create(directory, Size.MB.of(25));
        this.segment = new Segment<>(storage, new HistorySerializer(), dataStream, magic, WriteMode.LOG_HEAD);
    }

    public void append(HistoryItem item) {

    }

}
