package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.util.Size;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class GithubPumpFromRaw {

    private static final File directory = new File("J:\\github\\");
    private static final ExecutorService parsers = Executors.newFixedThreadPool(2);
    private static final ExecutorService writer = Executors.newSingleThreadExecutor();

    private static final IEventStore store = EventStore.open(new File("J:\\github-store\\"));

    public static void main(String[] args) {

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
                    parsers.submit(() -> {
                        try {
                            importFromFile(new File(directory, fileName));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                });

    }

    private static void importFromFile(File file) throws Exception {
        String stream = "github";
        String evType = "evType";

        long start = System.currentTimeMillis();
        List<EventRecord> records = new ArrayList<>();
        try (BufferedReader reader = openReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                EventRecord record = EventRecord.create(stream, evType, line);
                records.add(record);
            }
            System.out.println("PARSED " + records.size() + " in " + (System.currentTimeMillis() - start));
        }
        write(records);


    }

    private static void write(List<EventRecord> records) {
        writer.submit(() -> {
            long start = System.currentTimeMillis();
            AtomicInteger entries = new AtomicInteger();
            for (EventRecord rec : records) {
                store.append(rec);
                entries.incrementAndGet();
            }
            System.out.println("WRITE: " + entries + " in: " + (System.currentTimeMillis() - start));
        });
    }

    private static BufferedReader openReader(File file) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file)), Size.MB.intOf(5));
    }

    private static class Tuple<A, B> {
        public final A a;
        public final B b;

        private Tuple(A a, B b) {
            this.a = a;
            this.b = b;
        }
    }
}
