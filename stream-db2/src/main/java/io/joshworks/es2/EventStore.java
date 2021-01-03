package io.joshworks.es2;

import io.joshworks.es2.log.TLog;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.SSTables;
import io.joshworks.fstore.core.util.Size;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class EventStore {

    private final MemTable memTable;
    private final SSTables sstables;
    private final TLog tlog;

    public EventStore(Path root) {
        this.sstables = new SSTables(root.resolve("sstables"));
        this.tlog = new TLog(root.resolve("log"));
        this.memTable = new MemTable(Size.MB.ofInt(5), false);
    }

    public int version(long stream) {
        int currVersion = memTable.version(stream);
        if (currVersion == Event.NO_VERSION) {
            return sstables.version(stream);
        }
        return currVersion;
    }

    public int read(long stream, int startVersion, Sink sink) {
        int read = memTable.get(stream, startVersion, sink);
        if (read > 0) {
            return read;
        }
        return sstables.get(stream, startVersion, sink);
    }

    public void append(ByteBuffer event) {
        int eventVersion = Event.version(event);
        long stream = Event.stream(event);

        int currVersion = version(stream);
        int nextVersion = currVersion + 1;
        if (eventVersion != -1 && eventVersion != nextVersion) {
            throw new RuntimeException("Version mismatch");
        }

        Event.writeVersion(event, nextVersion);


        tlog.append(event);
        event.flip();
        if (!memTable.add(event)) {
            memTable.flush(sstables);
            tlog.roll();

            memTable.add(event);
        }
    }

}
