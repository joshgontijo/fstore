package io.joshworks.es2.directory;

import io.joshworks.es2.LengthPrefixedChannelIterator;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static io.joshworks.es2.SegmentChannel.create;
import static io.joshworks.es2.SegmentChannel.open;
import static io.joshworks.es2.directory.DirectoryUtils.segmentId;
import static io.joshworks.fstore.core.iterators.Iterators.closeableIterator;
import static io.joshworks.fstore.core.iterators.Iterators.stream;
import static java.nio.file.Files.exists;

public class Metadata<T extends SegmentFile> {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    private static final int EVENT_SIZE =
            Integer.BYTES + //op
                    Integer.BYTES + //level
                    Long.BYTES; //idx

    private static final int HEADER =
            Integer.BYTES + //len
                    Integer.BYTES + //crc
                    Long.BYTES; //timestamp


    private final SegmentChannel channel;

    public Metadata(File file) {
        channel = exists(file.toPath()) ? open(file) : create(file);
        long pos = restore();
        channel.position(pos);
        if (pos != channel.size()) {
            log.warn("Invalid data found in file event metadata, potential data loss");
            channel.truncate();
        }
    }

    private void append(List<FileEvent> events) {
        channel.append(serialize(events));
        channel.flush();
    }

    void add(T segment) {
        append(List.of(new FileEvent(Op.ADD, segmentId(segment))));
    }

    void merge(T out, List<T> sources) {
        List<FileEvent> events = createDeletes(sources);
        events.add(new FileEvent(Op.ADD, segmentId(out)));
        append(events);
    }

    void delete(List<T> items) {
        append(createDeletes(items));
    }

    private List<FileEvent> createDeletes(List<T> items) {
        List<FileEvent> events = new ArrayList<>();
        for (SegmentFile source : items) {
            events.add(new FileEvent(Op.DELETE, segmentId(source)));
        }
        return events;
    }


    private ByteBuffer serialize(List<FileEvent> events) {
        int eventsSerializedSize = events.size() * EVENT_SIZE;
        var bufferSize = HEADER + eventsSerializedSize;
        var buffer = Buffers.allocate(bufferSize, false);
        buffer.position(HEADER);
        for (FileEvent event : events) {
            buffer.putInt(event.op.i);
            buffer.putInt(event.segmentId.level());
            buffer.putLong(event.segmentId.idx());
        }

        assert !buffer.hasRemaining() : "Wrong buffer size";

        long timestamp = System.currentTimeMillis();
        int crc = ByteBufferChecksum.crc32(buffer, HEADER, eventsSerializedSize);
        //header
        buffer.position(0);
        buffer.putInt(bufferSize);
        buffer.putInt(crc);
        buffer.putLong(timestamp);

        buffer.clear();
        return buffer;
    }

    private static List<FileEvent> deserialize(ByteBuffer buffer) {
        int size = buffer.getInt(0);
        if (size != buffer.remaining()) {
            log.warn("Invalid file event buffer");
            return Collections.emptyList();
        }
        buffer.getInt(); //skip
        int crc = buffer.getInt();
        buffer.getLong(); //skip
        int computedChecksum = ByteBufferChecksum.crc32(buffer, buffer.position(), size - HEADER);
        if (computedChecksum != crc) {
            log.warn("File event checksum mismatch");
            return Collections.emptyList();
        }

        List<FileEvent> events = new ArrayList<>();
        while (buffer.hasRemaining()) {
            int op = buffer.getInt();
            int level = buffer.getInt();
            long idx = buffer.getLong();
            events.add(new FileEvent(Op.from(op), new SegmentId(level, idx)));
        }
        return events;
    }

    HashSet<SegmentId> state() {
        return read()
                .reduce(new HashSet<>(), (list, curr) -> {
                    switch (curr.op) {
                        case ADD:
                            if (!list.add(curr.segmentId)) {
                                throw new RuntimeException("Invalid event state");
                            }
                            break;
                        case DELETE:
                            if (!list.remove(curr.segmentId)) {
                                throw new RuntimeException("Invalid event state");
                            }
                    }
                    return list;
                }, (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                });
    }

    Stream<FileEvent> read() {
        return stream(closeableIterator(new LengthPrefixedChannelIterator(channel)))
                .map(Metadata::deserialize)
                .flatMap(Collection::stream);
    }

    private long restore() {
        long pos = 0;
        var it = new LengthPrefixedChannelIterator(channel);
        while (it.hasNext()) {
            ByteBuffer item = it.next();
            if (deserialize(item).isEmpty()) {
                break;
            }
            pos = it.position();
        }
        return pos;
    }

    public void close() {
        channel.close();
    }

    public record FileEvent(Op op, SegmentId segmentId) {
    }

    public enum Op {
        ADD(1),
        DELETE(2);

        private final int i;

        Op(int i) {
            this.i = i;
        }

        private static Op from(int v) {
            if (v == ADD.i) return ADD;
            if (v == DELETE.i) return DELETE;
            throw new RuntimeException("Invalid enum value: " + v);
        }

    }


}
