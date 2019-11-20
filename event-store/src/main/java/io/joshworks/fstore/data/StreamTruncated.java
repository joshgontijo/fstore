//package io.joshworks.eventry.data;
//
//import io.joshworks.fstore.es.shared.EventId;
//import io.joshworks.fstore.es.shared.EventRecord;
//import io.joshworks.fstore.es.shared.streams.SystemStreams;
//import io.joshworks.fstore.serializer.json.JsonSerializer;
//
//public class StreamTruncated {
//
//    public final String stream;
//    public final int versionAtDeletion;
//
//    public static final String TYPE = EventId.SYSTEM_PREFIX + "STREAM_TRUNCATED";
//
//    private StreamTruncated(String stream, int versionAtDeletion) {
//        this.stream = stream;
//        this.versionAtDeletion = versionAtDeletion;
//    }
//
//    public static EventRecord create(String stream, int version) {
//        var data = JsonSerializer.toBytes(new StreamTruncated(stream, version));
//        return EventRecord.create(SystemStreams.STREAMS, TYPE, data);
//    }
//
//    public static StreamTruncated from(EventRecord record) {
//        return JsonSerializer.fromJson(record.data, StreamTruncated.class);
//    }
//
//}
