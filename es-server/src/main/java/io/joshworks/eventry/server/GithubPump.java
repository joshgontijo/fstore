package io.joshworks.eventry.server;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.RestClient;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class GithubPump {

    private static final Gson gson = new Gson();
    private static Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    private static final RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();
    private static final File directory = new File("J:\\GithubArchive");

    public static void main(String[] args) throws Exception {

        for (String fileName : directory.list()) {
            if (fileName.toLowerCase().endsWith(".json")) {
                importFromFile(new File(directory, fileName));
            }
        }
    }

    private static void importFromFile(File file) throws Exception {
        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger();
        try (Stream<String> lines = Files.lines(file.toPath())) {
            lines.map(line -> {
                Map<String, Object> map = gson.fromJson(line, type);
                return map;
            }).map(map -> {
                Map<String, Object> wrapper = new HashMap<>();
                wrapper.put("type", map.get("type"));
                wrapper.put("data", map);
                return wrapper;
            })
            .forEach(map -> {
                HttpResponse<String> response = client.post("/streams/github")
                        .contentType(MediaType.APPLICATION_JSON_TYPE)
                        .body(map)
                        .asString();
                if (response.getStatus() != 201) {
                    System.err.println("Failed " + response.getBody());
                }
                counter.incrementAndGet();
            });
        }
        System.out.println(counter.get() + " entries imported from " + file.getName() + " in: " + (System.currentTimeMillis() - start));
    }

}
