package io.joshworks.eventry.it;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class GithubFileParser {


    private static final File directory = new File("E:\\Github");
    private static final File outputDirectory = new File(directory, "parsed");
    private static final ExecutorService executor = Executors.newFixedThreadPool(100);

    public static void main(String[] args) throws IOException, InterruptedException {

        if (!Files.exists(outputDirectory.toPath())) {
            Files.createDirectory(outputDirectory.toPath());
        }

        AtomicInteger processed = new AtomicInteger();
        AtomicInteger total = new AtomicInteger();

        for (String fileName : directory.list()) {
            if (fileName.toLowerCase().endsWith(".json")) {
                total.incrementAndGet();
                executor.execute(() -> {
                    try {
                        parseFile(new File(directory, fileName), new File(outputDirectory, fileName));
                        System.out.println("Completed " + processed.incrementAndGet() + "/" + total.get());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        System.out.println("All tasks submitted awaiting completion...");
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
    }

    private static void parseFile(File file, File output) throws Exception {

        final Gson gson = new Gson();


        long start = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(output), 1021*1024);
             Stream<String> lines = Files.lines(file.toPath())) {
            lines.map(line -> toMap(gson, line))
                    .map(GithubFileParser::mapJson)
                    .forEach(map -> {
                        writeLine(gson, writer, map);
                        counter.incrementAndGet();
                    });
            writer.flush();
        }
        System.out.println(counter.get() + " entries parsed from " + file.getName() + " in: " + (System.currentTimeMillis() - start));
    }

    private static Map<String, Object> toMap(Gson gson, String line) {
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> map = gson.fromJson(line, type);
        return map;
    }

    private static Map<String, Object> mapJson(Map<String, Object> map) {
        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("type", map.get("type"));
        wrapper.put("body", map);
        return wrapper;
    }

    private static void writeLine(Gson gson, BufferedWriter writer, Map<String, Object> map) {
        try {
            writer.write(gson.toJson(map));
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
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
//                String body = gson.dataAsJson(map);
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
