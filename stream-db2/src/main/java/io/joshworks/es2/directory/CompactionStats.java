package io.joshworks.es2.directory;

public record CompactionStats(long createdTime, long queueTime, long executionTime, long sourceSize, long outSize) {

}
