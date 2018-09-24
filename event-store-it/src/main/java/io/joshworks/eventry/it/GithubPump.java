package io.joshworks.eventry.it;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.RestClient;
import io.joshworks.restclient.request.body.RequestBodyEntity;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GithubPump {

    private static final Gson gson = new Gson();
    private static Type type = new TypeToken<Map<String, Object>>() {
    }.getType();

    private static final File directory = new File("J:\\GithubArchive");
    private static final ExecutorService executor = Executors.newFixedThreadPool(50);

    public static void main(String[] args) throws Exception {

        for (String fileName : directory.list()) {
            if (fileName.toLowerCase().endsWith(".json")) {
                executor.execute(() -> {
                    try {
                        RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();
                        importFromFile(client, new File(directory, fileName));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            }
        }
    }

    private static void importFromFile(RestClient client, File file) throws Exception {

        AtomicInteger counter = new AtomicInteger();
        try (Stream<String> lines = Files.lines(file.toPath())) {
            List<RequestBodyEntity> requests = lines.map(line -> {
                Map<String, Object> map = gson.fromJson(line, type);
                return map;
            }).map(map -> {
                Map<String, Object> wrapper = new HashMap<>();
                wrapper.put("type", map.get("type"));
                wrapper.put("data", map);
                return wrapper;
            }).map(map -> client.post("/streams/github").contentType(MediaType.APPLICATION_JSON_TYPE).body(map))
                    .collect(Collectors.toList());



            long start = System.currentTimeMillis();
            requests.forEach(req -> {
                HttpResponse<String> response = req.asString();
                if (response.getStatus() != 201) {
                    System.err.println("Failed " + response.getBody());
                }
            });
            System.out.println(requests.size() + " entries imported from " + file.getName() + " in: " + (System.currentTimeMillis() - start));

        }

    }


//    private static void importFromFileDirect(IEventStore store, File file) throws Exception {
//        long start = System.currentTimeMillis();
//        AtomicInteger counter = new AtomicInteger();
//        try (Stream<String> lines = Files.lines(file.toPath())) {
//
//            long parseStart = System.currentTimeMillis();
//
//            List<EventRecord> records = lines.map(line -> {
//                Map<String, Object> map = gson.fromJson(line, type);
//                return map;
//            }).map(map -> {
//                String data = gson.toJson(map);
//                return EventRecord.create("github", String.valueOf(map.get("type")), data);
//            }).collect(Collectors.toList());
//
//            long parseEnd = System.currentTimeMillis();
//
//            records.forEach(store::append);
//
//            long appendEnd = System.currentTimeMillis();
//
//            System.out.println(records.size() + " ITEMS -> PARSE: " + (parseEnd - parseStart) + " -> APPEND: " + (appendEnd - parseEnd));
//
//        }
//    }

}
