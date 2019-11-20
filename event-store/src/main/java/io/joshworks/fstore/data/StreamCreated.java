//package io.joshworks.eventry.data;
//
//import io.joshworks.fstore.es.shared.EventId;
//import io.joshworks.fstore.es.shared.EventRecord;
//import io.joshworks.eventry.stream.StreamMetadata;
//import io.joshworks.fstore.es.shared.streams.SystemStreams;
//import io.joshworks.fstore.serializer.json.JsonSerializer;
//
//public class StreamCreated {
//
//
//    public static final String TYPE = EventId.SYSTEM_PREFIX + "STREAM_CREATED";
//
//    public static EventRecord create(StreamMetadata metadata) {
//        //serializing straight into a StreamMetadata
//        var data = JsonSerializer.toBytes(metadata);
//        return EventRecord.create(SystemStreams.STREAMS, TYPE, data);
//    }
//
//    public static StreamMetadata from(EventRecord record) {
//        return JsonSerializer.fromJson(record.data, StreamMetadata.class);
//    }
//
//}
