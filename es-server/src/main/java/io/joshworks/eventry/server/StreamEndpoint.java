package io.joshworks.eventry.server;

import io.joshworks.eventry.api.IEventStore;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.tcp.EventCreated;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.StreamData;
import io.joshworks.fstore.es.shared.utils.StringUtils;
import io.joshworks.snappy.http.MediaType;
import io.joshworks.snappy.http.Request;
import io.joshworks.snappy.http.Response;

import java.util.List;
import java.util.stream.Collectors;

import static io.joshworks.snappy.http.Response.badRequest;
import static io.joshworks.snappy.http.Response.created;
import static io.joshworks.snappy.http.Response.notFound;
import static io.joshworks.snappy.http.Response.ok;

public class StreamEndpoint {

    public static final MediaType EVENTS_MEDIA_TYPE = MediaType.valueOf("applications/vnd.eventry.events+json");
    public static final String QUERY_PARAM_ZIP = "zip";
    public static final String EVENT_TYPE_HEADER = "Event-Type";
    public static final String QUERY_PARAM_ZIP_PREFIX = "prefix";
    public static final String PATH_PARAM_STREAM = "streamId";
    private final IEventStore store;

    public static final int DEFAULT_LIMIT = 100;

    public StreamEndpoint(IEventStore store) {
        this.store = store;
    }

    public Response create(Request request) {
        StreamInfo metadataBody = request.body().asObject(StreamInfo.class);
        StreamMetadata stream = store.createStream(metadataBody.name, metadataBody.maxCount, metadataBody.maxAge, metadataBody.permissions, metadataBody.metadata);
        return created(stream);
    }

    public Response allStreams(Request request) {
        List<StreamData> streamDataList = store.streamsMetadata().stream().map(metadata -> new StreamData(metadata.name, metadata.maxCount, metadata.maxAge, metadata.permissions, metadata.metadata))
                .collect(Collectors.toList());

        return ok(streamDataList);
    }

    public Response streamMetadata(Request request) {
        String stream = request.pathParameter("stream");
        if (StringUtils.isBlank(stream)) {
            return badRequest();
        }

        StreamInfo metadata = store.streamMetadata(stream).get();

        StreamData data = new StreamData(metadata.name, metadata.maxCount, metadata.maxAge, metadata.permissions, metadata.metadata);
        return ok(data);
    }

    public Response append(Request request) {
        String type = request.header(EVENT_TYPE_HEADER);
        if (StringUtils.isBlank(type)) {
            return badRequest();
        }
        String stream = request.pathParameter("stream");
        if (StringUtils.isBlank(stream)) {
            return badRequest();
        }

        byte[] eventData = request.body().asByteArray();
        EventRecord record = EventRecord.create(stream, type, eventData);
        EventRecord created = store.append(record);

        EventCreated eventCreated = new EventCreated(created.timestamp, created.version);
        return created(eventCreated);
    }


    public Response event(Request request) {
        String param = request.pathParameter("stream");
        EventId eventId = EventId.parse(param);
        EventRecord record = store.get(eventId);
        return record == null ? notFound() : ok(record.asJson());
    }
}
