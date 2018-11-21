package io.joshworks.eventry.server;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.snappy.http.HttpException;
import io.joshworks.snappy.http.HttpExchange;
import io.joshworks.snappy.http.MediaType;
import io.undertow.util.HeaderValues;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamEndpoint {

    public static final MediaType EVENTS_MEDIA_TYPE = MediaType.valueOf("applications/vnd.eventry.events+json");
    public static final String QUERY_PARAM_ZIP = "zip";
    public static final String EVENT_TYPE_HEADER = "Event-Type";
    public static final String QUERY_PARAM_ZIP_PREFIX = "prefix";
    public static final String PATH_PARAM_STREAM = "streamId";
    private final IEventStore store;

    public static final int DEFAULT_LIMIT = 100;
    public static final int DEFAULT_VERSION = 0;

    public StreamEndpoint(IEventStore store) {
        this.store = store;
    }

    public void create(HttpExchange exchange) {
        NewStream metadataBody = exchange.body().asObject(NewStream.class);
        store.createStream(metadataBody.name, metadataBody.maxCount, metadataBody.maxAge, metadataBody.permissions, metadataBody.metadata);
        exchange.send(201);
    }

    public void fetchStream(HttpExchange exchange) {
        String streamName = exchange.pathParameter(PATH_PARAM_STREAM);
        int limit = exchange.queryParameterVal("limit").asInt().orElse(DEFAULT_LIMIT);
        int startVersion = exchange.queryParameterVal("startVersion").asInt().orElse(Range.START_VERSION);

        if (SystemStreams.ALL.equals(streamName)) {
            //startVersion doesnt apply to _all
            try (Stream<EventRecord> stream = store.fromAll(LinkToPolicy.RESOLVE, SystemEventPolicy.INCLUDE).stream()) {
                List<EventBody> all = stream.filter(ev -> !ev.isLinkToEvent())
                        .limit(limit)
                        .map(EventBody::from)
                        .collect(Collectors.toList());
                exchange.send(all);
                return;
            }
        }

        try (Stream<EventRecord> stream = store.fromStream(StreamName.create(streamName, startVersion)).stream()) {
            List<EventBody> streamEvents = stream.filter(ev -> !ev.isLinkToEvent())
                    .limit(limit)
                    .map(EventBody::from)
                    .collect(Collectors.toList());
            exchange.send(streamEvents);
        }

    }

    public void streamsQuery(HttpExchange exchange) {

        String zipWithPrefix = extractZipStartingWith(exchange);
        Set<StreamName> streams = extractZipParams(exchange);
        int limit = exchange.queryParameterVal("limit").asInt().orElse(DEFAULT_LIMIT);

        if (!streams.isEmpty() && zipWithPrefix != null) {
            throw new HttpException(400, QUERY_PARAM_ZIP + " and " + QUERY_PARAM_ZIP_PREFIX + " cannot be used together");
        }

        //TODO check access to the stream
        List<EventBody> events = new ArrayList<>();
        if (!streams.isEmpty()) {
            try (Stream<EventRecord> recordStream = store.fromStreams(streams).stream()) {
                events = recordStream
                        .map(EventBody::from)
                        .limit(limit)
                        .collect(Collectors.toList());
            }


        } else if (zipWithPrefix != null) {
            try (Stream<EventRecord> recordStream = store.fromStreams(zipWithPrefix + Streams.STREAM_WILDCARD).stream()) {
                events = recordStream.limit(limit)
                        .map(EventBody::from)
                        .collect(Collectors.toList());
            }

        }
        exchange.send(events);
    }

    public void append(HttpExchange exchange) {
        String stream = exchange.pathParameter(PATH_PARAM_STREAM);
        if (MediaType.APPLICATION_OCTET_STREAM_TYPE.isCompatible(exchange.type())) {
            //simple json body, header must be present
            HeaderValues header = exchange.header(EVENT_TYPE_HEADER);
            if (header == null || header.isEmpty()) {
                //header must be provided
                //TODO add message
                exchange.status(400);
                return;
            }
            String eventType = header.get(0);
            if (StringUtils.isBlank(eventType)) {
                //TODO add message
                exchange.status(400);
                return;
            }

            byte[] eventBody = toBytes(exchange.body().asBinary());

            EventRecord record = EventRecord.create(stream, eventType, eventBody);
            store.append(record);

            exchange.status(201).end();
            return;
        }

        EventBody eventBody = exchange.body().asObject(EventBody.class);

        //TODO fix toEvent metadata when is empty
        EventRecord event = eventBody.toEvent(stream);
        store.append(event);

        exchange.status(201).end();
    }

    private static byte[] toBytes(InputStream is) {
        int bufferSize = 16384;
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream(bufferSize)) {
            int nRead;
            byte[] data = new byte[bufferSize];

            while ((nRead = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            return buffer.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void delete(HttpExchange exchange) {

    }

    public void listStreams(HttpExchange exchange) {
        List<StreamInfo> streamsMetadata = store.streamsMetadata();
        exchange.send(streamsMetadata);
    }

    public void metadata(HttpExchange exchange) {
        String stream = exchange.pathParameter(PATH_PARAM_STREAM);
        Optional<StreamInfo> metadata = store.streamMetadata(stream);
        metadata.ifPresentOrElse(exchange::send, () -> exchange.send(new HttpException(404, "Stream not found for " + stream)));
    }

    private Set<StreamName> extractZipParams(HttpExchange exchange) {
        Deque<String> zip = exchange.queryParameters(QUERY_PARAM_ZIP);
        Set<StreamName> streams = new HashSet<>();
        if (zip != null && !zip.isEmpty()) {
            for (String val : zip) {
                if (val != null && !val.isEmpty()) {
                    streams.add(StreamName.of(val));
                }
            }
        }
        return streams;
    }

    private String extractZipStartingWith(HttpExchange exchange) {
        return exchange.queryParameter(QUERY_PARAM_ZIP_PREFIX);
    }
}
