package io.joshworks.eventry.api;

import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface IStream {

    StreamMetadata createStream(String stream);

    StreamMetadata createStream(String stream, int maxCount, int maxAge, Map<String, Integer> acl, Map<String, String> metadata);

    List<StreamInfo> streamsMetadata();

    Set<Long> streams();

    Optional<StreamInfo> streamMetadata(String stream);

    void truncate(String stream, int fromVersion);

}
