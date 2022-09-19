package io.joshworks.es2.utils;

import io.joshworks.es2.Event;
import io.joshworks.es2.EventStore;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.StreamBlock;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;

public class SSTableDump {
    private SSTableDump() {
    }

    public static void dumpStream(long stream, EventStore store, File out) throws Exception {
        int totalBlocks = 0;
        int totalEntries = 0;
        try (var bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out)))) {
            int currVersion = 0;
            Sink.Memory sink = new Sink.Memory();
            int read = store.read(stream, currVersion, sink);
            while (read > 0) {
                ByteBuffer blockData = ByteBuffer.wrap(sink.data());

                int startVersion = StreamBlock.startVersion(blockData);
                int entries = StreamBlock.entries(blockData);
                currVersion = startVersion + entries;

                totalBlocks++;
                bw.append("----- ").append(StreamBlock.toString(blockData));
                bw.newLine();

                try (var iterator = StreamBlock.iterator(blockData)) {
                    while (iterator.hasNext()) {
                        var item = iterator.next();
                        totalEntries++;
                        bw.append(Event.toString(item));
                        bw.newLine();
                    }
                }

                bw.newLine();
                bw.newLine();

                sink.close();
                read = store.read(stream, currVersion, sink);
            }

            bw.newLine();
            bw.newLine();
            bw.append("TOTAL_BLOCKS: ")
                    .append(String.valueOf(totalBlocks))
                    .append(", TOTAL_ENTRIES: ")
                    .append(String.valueOf(totalEntries));


        }

    }

}
