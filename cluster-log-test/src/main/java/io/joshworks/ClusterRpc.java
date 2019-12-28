package io.joshworks;

public interface ClusterRpc {

    void becomeLeader();

    void becomeFollower(String nodeId);

    void truncateLog(String nodeId);

    long getCommitIndex();
}
