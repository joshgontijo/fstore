package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.fstore.core.properties.AppProperties;
import io.joshworks.snappy.http.MediaType;
import io.joshworks.snappy.parser.Parsers;
import io.joshworks.snappy.parser.PlainTextParser;

import java.io.File;

import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.delete;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.onShutdown;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.put;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.parser.MediaTypes.consumes;
import static io.joshworks.snappy.parser.MediaTypes.produces;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();
        String path = properties.get("store.path").orElse("J:\\github-store");
        IEventStore store = EventStore.open(new File(path));

        EventBroadcaster broadcast = new EventBroadcaster(2000, 3);
        SubscriptionEndpoint subscriptions = new SubscriptionEndpoint(store, broadcast);
        StreamEndpoint streams = new StreamEndpoint(store);
        ProjectionsEndpoint projections = new ProjectionsEndpoint(store);


        Parsers.register(MediaType.valueOf(JAVASCRIPT_MIME), new PlainTextParser());

        group("/streams", () -> {
            post("/", streams::create);
            get("/", streams::streamsQuery);
            get("/metadata", streams::listStreams);

            group("{streamId}", () -> {
                get(streams::fetchStream);
                post(streams::append, consumes(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM));
                delete(streams::delete);
                get("/metadata", streams::metadata);
            });
        });

        group("/projections", () -> {
            get(projections::getAll);
            post(projections::create, consumes(JAVASCRIPT_MIME));
            post("AD-HOC-QUERY-TODO", projections::create);
            group("{name}", () -> {
                put(projections::update, consumes(JAVASCRIPT_MIME));
                get(projections::get);
                delete(projections::delete);
                get("script", projections::getScript, produces(JAVASCRIPT_MIME));
                post("stop", projections::stop);
                post("start", projections::run);
                post("resume", projections::resume);
                post("disable", projections::disable);
                post("enable", projections::enable);
                get("status", projections::executionStatus);

            });
        });

        group("/push", () -> sse(subscriptions.newPushHandler()));


        onShutdown(store::close);

        cors();
        start();

    }
}
