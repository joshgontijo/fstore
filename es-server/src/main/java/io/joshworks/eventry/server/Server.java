package io.joshworks.eventry.server;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.server.subscription.Subscriptions;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.AppProperties;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.snappy.sse.SseCallback;
import io.undertow.server.handlers.sse.ServerSentEventConnection;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.joshworks.snappy.SnappyServer.adminPort;
import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.port;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.SnappyServer.staticFiles;
import static io.joshworks.snappy.http.Response.ok;
import static io.joshworks.snappy.parser.MediaTypes.produces;

public class Server {

    private static final String JAVASCRIPT_MIME = "application/javascript";

    private static final Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();


    public static void main(String[] args) {

        AppProperties properties = AppProperties.create();
        String path = properties.get("store.path").orElse("S:\\es-server");
        int serverPort = properties.getInt("port").orElseThrow(() -> new RuntimeException("Port must be provided"));

        FileUtils.tryDelete(new File(path));

        port(serverPort);
        adminPort(serverPort + 10);

        ClusterStore store = ClusterStore.connect(new File(path), "es-cluster", serverPort);

        StreamEndpoint streams = new StreamEndpoint(store);
        Subscriptions subscriptions = new Subscriptions(3, 3000, 5);

        staticFiles("/", "static");

        group("/nodes", () -> {
            get(req -> ok(store.nodesInfo()));
        });

        group("/streams", () -> {
            get(streams::allStreams, produces("json"));
            post(streams::create, produces("json"));
            get("/{stream}/metadata", streams::streamMetadata, produces("json"));
            get("{stream}", streams::event, produces("json"));
            post("{stream}", streams::append, produces("json"));
        });

        group("/from-stream", () -> {

        });

        get("/nodelog", req -> {
            List<EventRecord.JsonView> items = store.nodeLog().iterator().stream().map(EventRecord::asJson).collect(Collectors.toList());
            return ok(items);
        }, produces("json"));

        group("/subscriptions", () -> {
//            put("{id}", req -> {
//                String subscriptionId = req.pathParameter("id");
//                Subscription subscription = req.body().asObject(Subscription.class);
//                EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(subscription.pattern));
//                subscriptions.add(subscriptionId, iterator);
//                return created();
//            });

            sse("{subscriptionId}", new SseCallback() {
                @Override
                public void connected(ServerSentEventConnection connection, String lastEventId) {
                    String subscriptionId = connection.getParameter("subscriptionId");
                    String pattern = connection.getQueryParameters().get("stream").getFirst();

                    EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(pattern));
                    boolean started = subscriptions.create(subscriptionId, iterator, connection);
                    if (!started) {
                        IOUtils.closeQuietly(connection);
                    }
                }

                @Override
                public void onClose(ServerSentEventConnection connection) {
                    String subscriptionId = connection.getParameter("subscriptionId");
                    subscriptions.remove(subscriptionId);
                }
            });
        });

        cors();
        start();
    }


}
