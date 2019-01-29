package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.StreamName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Partitions {

    private static final Logger logger = LoggerFactory.getLogger(Partitions.class);
    private static final int PARTITIONS = 1000;
    private static final String FILE = ".partitions";

    private final List<Partition> partitions = new ArrayList<>();

    public Partitions(File rootDir, IEventStore localStore) {
        //nothing
        if(!new File(rootDir, FILE).exists()) {
            logger.info("Creating partitions");
            for (int i = 0; i < PARTITIONS; i++) {
                partitions.add(new Partition(i, localStore));
            }
        }
    }

    public Partition select(String stream) {
        long hash = StreamName.hash(stream);
        int idx = (int) (Math.abs(hash) % PARTITIONS);
        return partitions.get(idx);
    }
}
