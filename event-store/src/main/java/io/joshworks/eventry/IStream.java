package io.joshworks.eventry;

import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IStream {

    void createStream(String name);

    void createStream(String name, int maxCount, long maxAge);

    StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata);

    List<StreamInfo> streamsMetadata();

    Optional<StreamInfo> streamMetadata(String stream);

    void truncate(String stream, int fromVersion);

}
