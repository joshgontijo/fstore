package io.joshworks;

import io.joshworks.fstore.cluster.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;


public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    private String leader;
    private final Set<String> followers = new ConcurrentSkipListSet<>();
    private final Cluster cluster;
    private final CommitTable commitTable;


    public Coordinator(Cluster cluster, CommitTable commitTable) {
        this.cluster = cluster;
        this.commitTable = commitTable;
    }

    public String leader() {
        return leader;
    }

    public boolean tryLeaderShip(String node) {
        long newNodeSequence = commitTable.get(node);
        long currLeaderSequence = commitTable.get(commitTable.highestSequenceNode());
        boolean electedNewLeader = newNodeSequence > currLeaderSequence || leader == null;
        if (electedNewLeader) {
            leader = node;
        }
        return electedNewLeader;
    }

    //Basic implementation: Higher sequence becomes leader
    //TODO implement conflicting entries if more than one one have more entries than highWaterMark
    public boolean tryElectLeader(Cluster cluster) {
        String newLeader = commitTable.highestSequenceNode();
        logger.info("Made {} leader", leader);
        boolean electedNewLeader = !newLeader.equals(leader);
        leader = newLeader;
        return electedNewLeader;

//
//        if (leader == null) { //no leader
//            leader = commitTable.highestSequenceNode();
//            logger.info("Made {} leader", leader);
//            return;
//        }
//        //TODO
//        long highWaterMark = commitTable.highWaterMark();
    }
}
