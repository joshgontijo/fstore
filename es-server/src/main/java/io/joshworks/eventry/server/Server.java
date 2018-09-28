package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.QueuedEventStore;
import io.joshworks.fstore.core.properties.AppProperties;
import io.joshworks.snappy.http.MediaType;

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


    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();
        String path = properties.get("store.path").orElse("J:\\event-store-github");
        IEventStore store = new QueuedEventStore(EventStore.open(new File(path)));

        EventBroadcaster broadcast = new EventBroadcaster(2000, 3);
        SubscriptionEndpoint subscriptions = new SubscriptionEndpoint(store, broadcast);
        StreamEndpoint streams = new StreamEndpoint(store);
        ProjectionsEndpoint projections = new ProjectionsEndpoint(store);


        group("/streams", () -> {
            post("/", streams::create);
            get("/", streams::streamsQuery);
            get("/metadata", streams::listStreams);

            group("{streamId}", () -> {
                get(streams::fetchStreams);
                post(streams::append, consumes(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM));
                delete(streams::delete);
                get("/metadata", streams::metadata);
            });
        });

        group("/projections", () -> {
            get(projections::getAll);
            post(projections::create);
            post("AD-HOC-QUERY-TODO", projections::create);
            group("{name}", () -> {
                put(projections::update);
                get(projections::get);
                delete(projections::delete);
                group("/script", () -> {
                    put(projections::updateScript, consumes("application/javascript"));
                    get(projections::getScript, produces("application/javascript"));
                });
                group("/executions", () -> {
                    post(projections::run);
                    get(projections::executionStatus);
                });

            });
        });

        group("/push", () -> sse(subscriptions.newPushHandler()));


        onShutdown(store::close);

        cors();
        start();

    }
}
