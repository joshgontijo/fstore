package io.joshworks.eventry.index.midpoint;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.utils.Memory;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Midpoints {

    private static final Serializer<Midpoint> midpointSerializer = new MidpointSerializer();
    private final List<Midpoint> midpoints;
    private final File handler;
    boolean dirty;

    public Midpoints(File indexDir, String segmentFileName) {
        this.handler = getFile(indexDir, segmentFileName);
        this.midpoints = load(handler);
    }

    public void add(Midpoint start, Midpoint end) {
        if (midpoints.isEmpty()) {
            midpoints.add(start);
            midpoints.add(end);
        } else {
            midpoints.set(midpoints.size() - 1, start);
            midpoints.add(end);
        }
        dirty = true;

    }

    public void add(Midpoint midpoint) {
        Objects.requireNonNull(midpoint, "Midpoint cannot be null");
        Objects.requireNonNull(midpoint.key, "Midpoint entry cannot be null");
        this.midpoints.add(midpoint);
        dirty = true;
    }

    public void write() {
        if (!dirty) {
            return;
        }

        int totalRecordSize = Integer.BYTES;
        long size = Math.max((long) Midpoint.BYTES * midpoints.size(), handler.length()) + totalRecordSize;

        try (Storage storage = StorageProvider.of(StorageMode.RAF).create(handler, size)) {
            var sizeMarker = ByteBuffer.allocate(Integer.BYTES);
            sizeMarker.putInt(midpoints.size()).flip();
            storage.write(sizeMarker);
            for (Midpoint midpoint : midpoints) {
                ByteBuffer data = midpointSerializer.toBytes(midpoint);
                storage.write(data);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write midpoints", e);
        }
        dirty = false;
    }


    private List<Midpoint> load(File handler) {
        if (!handler.exists()) {
            return new ArrayList<>();
        }
        try (Storage storage = StorageProvider.of(StorageMode.RAF).open(handler)) {
            long pos = 0;
            var data = ByteBuffer.allocate((Memory.PAGE_SIZE / Midpoint.BYTES) * Midpoint.BYTES);

            var sizeData = ByteBuffer.allocate(Integer.BYTES);
            storage.read(pos, sizeData);
            pos += Integer.BYTES;
            int entries = sizeData.flip().getInt();

            List<Midpoint> loadedMidpoints = new ArrayList<>(entries);
            int loaded = 0;
            while (loaded < entries) {
                data.clear();
                int read = storage.read(pos, data);
                if (read <= 0) {
                    break;
                }
                data.flip();
                while (data.remaining() >= Midpoint.BYTES) {
                    Midpoint midpoint = midpointSerializer.fromBytes(data);
                    loadedMidpoints.add(midpoint);
                    pos += Midpoint.BYTES;
                    loaded++;
                    if(loaded >= entries) {
                        break;
                    }
                }
            }

            return loadedMidpoints;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load midpoints", e);
        }
    }

    private static File getFile(File indexDir, String segmentName) {
        return new File(indexDir, segmentName.split("\\.")[0] + ".mdp");
    }

    public int getMidpointIdx(IndexEntry entry) {
        int idx = Collections.binarySearch(midpoints, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to queuedTime scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= midpoints.size()) {
            throw new IllegalStateException("Got index " + idx + " midpoints position: " + midpoints.size());
        }
        return idx;
    }

    public Midpoint getMidpointFor(IndexEntry entry) {
        int midpointIdx = getMidpointIdx(entry);
        if (midpointIdx >= midpoints.size() || midpointIdx < 0) {
            return null;
        }
        return midpoints.get(midpointIdx);
    }


    public void delete() {
        try {
            Files.deleteIfExists(handler.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean inRange(Range range) {
        if (midpoints.isEmpty()) {
            return false;
        }
        return !(range.start().compareTo(last()) > 0 || range.end().compareTo(first()) < 0);
    }

    public int size() {
        return midpoints.size();
    }

    public IndexEntry first() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return firstMidpoint().key;
    }

    public boolean isEmpty() {
        return midpoints.isEmpty();
    }

    public IndexEntry last() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return lastMidpoint().key;
    }

    private Midpoint firstMidpoint() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(0);
    }

    private Midpoint lastMidpoint() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(midpoints.size() - 1);
    }


}
