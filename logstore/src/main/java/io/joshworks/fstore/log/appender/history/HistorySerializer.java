package io.joshworks.fstore.log.appender.history;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.appender.history.data.HistoryItem;

import java.nio.ByteBuffer;

public class HistorySerializer implements Serializer<HistoryItem> {

    @Override
    public void writeTo(HistoryItem data, ByteBuffer dst) {

    }

    @Override
    public HistoryItem fromBytes(ByteBuffer buffer) {
        return null;
    }
}
