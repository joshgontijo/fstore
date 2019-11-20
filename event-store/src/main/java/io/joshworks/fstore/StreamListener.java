package io.joshworks.fstore;

import io.joshworks.fstore.stream.StreamMetadata;

public interface StreamListener {

    void onStreamCreated(StreamMetadata metadata);

    void onStreamTruncated(StreamMetadata metadata);

    void onStreamDeleted(StreamMetadata metadata);

}
