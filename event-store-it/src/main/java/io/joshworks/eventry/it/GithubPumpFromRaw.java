package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GithubPumpFromRaw {

    private static final File directory = new File("J:\\github\\");
    private static final ExecutorService executor = Executors.newFixedThreadPool(50);

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
//                    executor.submit(() -> {
                        try {
                            importFromFile(new File(directory, fileName));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
//                    });
                });


    }

    private static void importFromFile(File file) throws Exception {
        long start = System.currentTimeMillis();
        String stream = "github";
        String evType = "evType";

        int entries = 0;
        try (BufferedReader reader = openReader(file)) {
            String line;
            while((line = reader.readLine()) != null) {
                EventRecord record = EventRecord.create(stream, evType, line);
                store.append(record);
                entries++;
            }
        }

        System.out.println(entries + " entries imported from " + file.getName() + " in: " + (System.currentTimeMillis() - start));
    }

    private static BufferedReader openReader(File file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
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
