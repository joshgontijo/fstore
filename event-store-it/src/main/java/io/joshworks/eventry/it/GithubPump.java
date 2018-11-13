package io.joshworks.eventry.it;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.RestClient;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class GithubPump {

    private static final File directory = new File("J:\\github\\");
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    private static ThreadLocal<RestClient> client = ThreadLocal.withInitial(() -> RestClient.builder().baseUrl("http://localhost:9000").build());

    public static void main(String[] args) throws Exception {

        final DateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");

        Arrays.asList(directory.list()).stream()
                .filter(name -> name.toLowerCase().endsWith(".json"))
                .map(name -> {
                    try {
                        String date = name.replaceAll(".json", "");
                        return new Tuple<>(name, format.parse(date));
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sorted(Comparator.comparing(o -> o.b))
                .map(t -> t.a)
                .forEach(fileName -> {

                    executor.execute(() -> {
                        try {
                            importFromFile(client.get(), new File(directory, fileName));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                });


        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.HOURS);


    }

    private static void importFromFile(RestClient client, File file) throws Exception {
        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger();

        final Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Object>>(){}.getType();

        try (Stream<String> lines = Files.lines(file.toPath())) {
            lines.filter(line -> line != null && !line.isEmpty())
                    .forEach(jsonString -> {

                        Map<String, Object> map = new HashMap<>();
                        Map<String, Object> data = gson.fromJson(jsonString, type);
                        map.put("type", data.get("type"));
                        map.put("body", data);

                        String formatted = gson.toJson(map);

                        HttpResponse<String> response = client.post("/streams/github")
                                .contentType(MediaType.APPLICATION_JSON_TYPE)
                                .body(formatted)
                                .asString();

                        if (response.getStatus() != 201) {
                            System.err.println("Failed " + response.getBody());
                            System.err.println("Data: " + jsonString);
                        }
                        counter.incrementAndGet();
                    });
        }
        System.out.println(counter.get() + " entries imported from " + file.getName() + " in: " + (System.currentTimeMillis() - start));
    }

    private static class Tuple<A, B> {
        public final A a;
        public final B b;

        private Tuple(A a, B b) {
            this.a = a;
            this.b = b;
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
//                String body = gson.toJson(map);
//                return EventRecord.create("github", String.valueOf(map.get("type")), body);
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
