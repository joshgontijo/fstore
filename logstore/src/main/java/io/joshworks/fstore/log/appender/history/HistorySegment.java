package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.Type;

public class HistorySegment extends Segment<HistoryItem> {

    public HistorySegment(Storage storage, Serializer<HistoryItem> serializer, IDataStream reader, String magic) {
        super(storage, serializer, reader, magic);
    }

    public HistorySegment(Storage storage, Serializer<HistoryItem> serializer, IDataStream reader, Type type, String magic) {
        super(storage, serializer, reader, magic, type);
    }
}
