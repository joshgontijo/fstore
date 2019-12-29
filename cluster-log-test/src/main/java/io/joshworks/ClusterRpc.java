package io.joshworks;

public interface ClusterRpc {

    void becomeLeader();

    void becomeFollower(String nodeId, int replPort);

    void truncateLog(String nodeId);

    long getCommitIndex();
}
