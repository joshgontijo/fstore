package io.joshworks.eventry;

import io.joshworks.eventry.stream.StreamMetadata;

public interface StreamListener {

    void onStreamCreated(StreamMetadata metadata);

    void onStreamTruncated(StreamMetadata metadata);

    void onStreamDeleted(StreamMetadata metadata);

}
