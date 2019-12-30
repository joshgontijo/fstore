package io.joshworks;

public interface ClusterRpc {

    void becomeLeader();

    void becomeFollower(String leader, Integer replPort);

    void becomeAvailable();

    void truncateLog(String nodeId);

    long getCommitIndex();

    int replicationPort();
}
