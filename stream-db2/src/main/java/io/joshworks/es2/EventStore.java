package io.joshworks.es2;

import io.joshworks.fstore.core.RuntimeIOException;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventStore {

    private final LogDir logs;
    private final LogDir sstables;

    public EventStore(Path root) {
        this.logs = new LogDir(root.resolve("logs"));
        this.sstables = new LogDir(root.resolve("sstables"));
    }

    public void append(ByteBuffer event) {
        int eventSize = Event.sizeOf(event);
        int expectedVersion = Event.version(event);


        if (expectedVersion != -1) {

        }
        SegmentChannel head = logs.head();
        long position = head.append(event);



    }


    private static class SegmentId {
        private final int level;
        private final long idx;

        private SegmentId(int level, long idx) {
            this.level = level;
            this.idx = idx;
        }
    }

    private static class LogDir {
        private static final String EXT = "log";
        private final Path folder;
        private final List<SegmentChannel> items = new CopyOnWriteArrayList<>();

        private LogDir(Path folder) {
            this.folder = folder;
        }

        private SegmentChannel head() {
            if (items.isEmpty()) {
                throw new IllegalStateException("No log head");
            }
            return items.get(items.size() - 1);
        }

        private SegmentId segmentId(SegmentChannel channel) {
            String name = channel.name();
            try {
                String[] nameExt = name.split("\\.");
                if (nameExt.length != 2) {
                    throw new IllegalStateException("Invalid segment name");
                }
                if (!EXT.equals(nameExt[1])) {
                    throw new IllegalStateException("Invalid segment extension");
                }
                String[] split = nameExt[0].split("-");
                if (split.length != 2) {
                    throw new IllegalStateException("Invalid segment name");
                }

                return new SegmentId(Integer.parseInt(split[0]), Long.parseLong(split[1]));

            } catch (Exception e) {
                throw new RuntimeIOException("Could not parse segment " + name, e);
            }
        }
    }

}
