//package io.joshworks.fstore.cli;
//
//
//import io.joshworks.fstore.client.StoreClient;
//import io.joshworks.fstore.es.shared.utils.StringUtils;
//import io.joshworks.fstore.serializer.json.JsonSerializer;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.InputStreamReader;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
//import java.net.URL;
//import java.util.Arrays;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import static io.joshworks.fstore.es.shared.utils.StringUtils.requireNonBlank;
//
//public class CLI {
//
//    private static final AtomicBoolean closed = new AtomicBoolean();
//    private static final Object RW_LOCK = new Object();
//    private static StoreClient client;
//
//    public static void main(String[] args) throws Exception {
//
//        String url = requireNonBlank(args[0], "URL");
//        client = StoreClient.connect(new URL(url));
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> client.close()));
//
//        try (var is = new BufferedReader(new InputStreamReader(System.in)); var out = new BufferedWriter(new OutputStreamWriter(System.out))) {
//            while (!closed.get()) {
//                String line;
//                synchronized (RW_LOCK) {
//                    out.newLine();
//                    out.write("> ");
//                    out.flush();
//                    line = is.readLine();
//                }
//                String[] parts = parseInput(line);
//                Command command = parseCommand(parts[0]);
//
//                try {
//                    command.execute(client, new Args(parts), out);
//                } catch (Exception e) {
//                    Throwable ex = e;
//                    while (ex.getCause() != null) {
//                        ex = ex.getCause();
//                    }
//                    out.append(e.getMessage());
//                }
//            }
//        }
//    }
//
//    private static String[] parseInput(String line) {
//        return Arrays.stream(line.split("\\s+"))
//                .filter(StringUtils::nonBlank)
//                .toArray(String[]::new);
//    }
//
//    private static Command parseCommand(String cmd) {
//        switch (cmd) {
//            case "print":
//                return new Print();
//            case "append":
//                return new Append();
//            case "exit":
//                return new Exit();
//            default:
//                throw new RuntimeException("No command");
//        }
//    }
//
//    private interface Command {
//        void execute(StoreClient client, CLI.Args args, Writer out);
//    }
//
//    private static class Print implements Command {
//
//        @Override
//        public void execute(StoreClient client, Args args, Writer out) {
//            System.out.println(args.get(0, "word"));
//        }
//    }
//
//    private static class Exit implements Command {
//
//        @Override
//        public void execute(StoreClient client, Args args, Writer out) {
//            //close stuff
//            client.close();
//            closed.set(true);
//        }
//    }
//
//    private static class Append implements Command {
//
//        @Override
//        public void execute(StoreClient client, Args args, Writer out) {
//            String stream = args.get(0, "stream");
//            String type = args.get(1, "type");
//            String data = args.get(1, "data");
//            client.append(stream, type, JsonSerializer.toMap(data));
//        }
//    }
//
//    private static class Args {
//        private final String[] items;
//
//        private Args(String[] items) {
//            this.items = new String[items.length - 1];
//            System.arraycopy(items, 1, this.items, 0, this.items.length);
//        }
//
//        public String get(int idx, String name) {
//            if (idx >= items.length) {
//                throw new RuntimeException(name + " not provided");
//            }
//            return items[idx];
//        }
//
//    }
//
//}