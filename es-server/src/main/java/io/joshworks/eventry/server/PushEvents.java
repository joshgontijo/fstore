package io.joshworks.eventry.server;

import com.google.gson.reflect.TypeToken;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.RestClient;
import io.joshworks.stream.client.StreamClient;
import io.joshworks.stream.client.sse.SSEConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PushEvents {

    public static void main(String[] args) throws InterruptedException {


        RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();
        for (int i = 0; i < 1000000; i++) {

            HttpResponse<String> response = client.post("/streams/users-1")
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .header("Event-Type", "USER_CREATED")
                    .header("Connection", "Keep-Alive")
                    .body(Map.of("name", "Josh", "age", 1, "gender", "m"))
                    .asString();

            if (!response.isSuccessful()) {
                System.err.println(response.getStatus() + " -> " + response.asString());
            }

        }

        System.out.println("READING....");

        List<AtomicLong> counters = new ArrayList<>();
        List<SSEConnection> conns = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final int idx = i;
            counters.add(new AtomicLong());
            SSEConnection conn = StreamClient.sse("http://localhost:9000/subscriptions/sub-" + i + "?stream=users-*")
                    .onOpen(() -> System.out.println("Opened connection"))
                    .onEvent(e -> {
                        var type = new TypeToken<List<EventRecord.JsonView>>() {
                        }.getType();
                        List<EventRecord.JsonView> records = JsonSerializer.fromJson(e.data, type);
                        counters.get(idx).addAndGet(records.size());
                    }).connect();

            conns.add(conn);
        }

        new Thread(() -> {
            while (true) {
                for (int i = 0; i < counters.size(); i++) {
                    System.out.println(i + " -> " + counters.get(i).get());
                }
                Threads.sleep(5000);
            }
        }).start();

        Thread.sleep(360000);

        conns.forEach(SSEConnection::close);


    }

}
