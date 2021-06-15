package io.joshworks.fstore.log.appender.history.data;

import java.util.List;

public class SegmentMerged {

    public final List<String> source;
    public final String output;
    public final long timestamp;

    public SegmentMerged(List<String> source, String output, long timestamp) {
        this.source = source;
        this.output = output;
        this.timestamp = timestamp;
    }
}
