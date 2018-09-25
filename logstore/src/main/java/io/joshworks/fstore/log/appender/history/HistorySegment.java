package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.log.reader.DataStream;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.Type;

public class HistorySegment extends Segment<HistoryItem> {

    public HistorySegment(Storage storage, DataStream<HistoryItem> dataStream, String magic) {
        super(storage, dataStream, magic);
    }

    public HistorySegment(Storage storage, DataStream<HistoryItem> dataStream, Type type, String magic) {
        super(storage, dataStream, magic, type);
    }
}
