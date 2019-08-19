package io.joshworks.eventry.server;

import io.joshworks.eventry.network.tcp.XTcpServer;
import io.joshworks.eventry.server.subscription.Subscriptions;
import io.joshworks.eventry.server.subscription.polling.LocalPollingSubscription;
import io.joshworks.eventry.server.tcp.TcpEventHandler;
import io.joshworks.fstore.core.util.AppProperties;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.subscription.SubscriptionOptions;
import io.joshworks.snappy.websocket.WebsocketEndpoint;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.joshworks.snappy.SnappyServer.adminPort;
import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.group;
import static io.joshworks.snappy.SnappyServer.ioThreads;
import static io.joshworks.snappy.SnappyServer.port;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.sse;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.SnappyServer.staticFiles;
import static io.joshworks.snappy.SnappyServer.websocket;
import static io.joshworks.snappy.SnappyServer.workerThreads;
import static io.joshworks.snappy.http.Response.ok;
import static io.joshworks.snappy.parser.MediaTypes.produces;

public class Server {

    public static void main(String[] args) {

        var properties = AppProperties.create();
        var path = properties.get("store.path").orElse("S:\\es-server");
        int httpPort = properties.getInt("es.http.port").orElseThrow(() -> new RuntimeException("Http port must be provided"));
        int tcpPort = properties.getInt("es.tcp.port").orElseThrow(() -> new RuntimeException("Tcp port must be provided"));

        FileUtils.tryDelete(new File(path));

        port(httpPort);
        adminPort(httpPort + 10);
        ioThreads(2);
        workerThreads(3, 3, 10000);

        var store = ClusterStore.connect(new File(path), "es-cluster", httpPort, tcpPort);

        var streams = new StreamEndpoint(store);
        var subscriptions = new Subscriptions(3, 3000, 5);

        var poolingSubscription = new LocalPollingSubscription(store.thisNode().store());
        XTcpServer tcpServer = XTcpServer.create()
                .onOpen(conn -> System.out.println("Connection opened"))
                .onClose(conn -> System.out.println("Connection closed"))
                .onIdle(conn -> System.out.println("Connection idle"))
                .idleTimeout(10, TimeUnit.SECONDS)
                .bufferSize(Size.MB.ofInt(10))
                .option(Options.REUSE_ADDRESSES, true)
                .option(Options.TCP_NODELAY, true)
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(256))
                .option(Options.WORKER_IO_THREADS, 1)
//                .option(Options.RECEIVE_BUFFER, Size.MB.ofInt(5))
                .onEvent(new TcpEventHandler(store, poolingSubscription))
                .start(new InetSocketAddress("localhost", tcpPort));




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
//                subscriptions.create(subscriptionId, iterator);
//                return created();
//            });

            get(req -> ok(subscriptions.info()));


            websocket("/ws", new WebsocketEndpoint() {
                @Override
                public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {

                }

            });

            sse("{subscriptionId}", sse -> {
                var subscriptionId = sse.pathParameter("subscriptionId");
                var patterns = new HashSet<>(sse.queryParameters("stream"));
                int batchSize = sse.queryParameterVal("batchSize").asInt().orElse(20);
                boolean compress = sse.queryParameterVal("compress").asBoolean().orElse(false);
//                boolean wrapEvent = sse.queryParameterVal("wrapEvent").asBoolean().orElse(false);

                var options = new SubscriptionOptions(patterns, batchSize, compress);

                var iterator = store.fromStreams(EventMap.empty(), patterns);
                boolean started = subscriptions.create(subscriptionId, options, iterator, sse);
                if (!started) {
                    sse.close();
                }

                sse.keepAlive(15000);
                sse.onClose(() -> subscriptions.remove(subscriptionId));
            });
        });

        cors();
        start();
    }


}
