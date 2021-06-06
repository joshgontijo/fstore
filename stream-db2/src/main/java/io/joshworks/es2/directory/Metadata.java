package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentChannel;

import java.io.File;

import static io.joshworks.es2.SegmentChannel.create;
import static io.joshworks.es2.SegmentChannel.open;
import static java.nio.file.Files.exists;

public class Metadata {

    private final SegmentChannel channel;

    public Metadata(File file) {
        channel = exists(file.toPath()) ? open(file) : create(file);
    }

    public void append(Event event) {

    }

    public abstract static class Event {

    }

    private static class SegmentId {

    }

}
