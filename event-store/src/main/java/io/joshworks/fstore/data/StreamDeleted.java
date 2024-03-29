//package io.joshworks.eventry.data;
//
//import io.joshworks.fstore.es.shared.EventId;
//import io.joshworks.fstore.es.shared.EventRecord;
//import io.joshworks.fstore.es.shared.streams.SystemStreams;
//import io.joshworks.fstore.serializer.json.JsonSerializer;
//
//public class StreamDeleted {
//
//    //serializing straight into a StreamMetadata
//
//    public final String stream;
//    public final int versionAtDeletion;
//
//    public static final String TYPE = EventId.SYSTEM_PREFIX + "STREAM_DELETED";
//
//    private StreamDeleted(String stream, int versionAtDeletion) {
//        this.stream = stream;
//        this.versionAtDeletion = versionAtDeletion;
//    }
//
//    public static EventRecord create(String stream, int versionAtDeletion) {
//        var data = JsonSerializer.toBytes(new StreamDeleted(stream, versionAtDeletion));
//        return EventRecord.create(SystemStreams.STREAMS, TYPE, data);
//    }
//
//    public static StreamDeleted from(EventRecord record) {
//        return JsonSerializer.fromJson(record.data, StreamDeleted.class);
//    }
//
//}
