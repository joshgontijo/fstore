package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterStoreTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final int NUM_PARTITIONS = 2;
    private ClusterStore node1; //all data should be written and read from this node
    private ClusterStore _anotherNode; //do not communicate with this noe other than verification
    private File testFile1;
    private File testFile2;

    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");
        testFile1 = FileUtils.testFolder();
        testFile2 = FileUtils.testFolder();
        node1 = ClusterStore.connect(testFile1, CLUSTER_NAME, NUM_PARTITIONS);
        _anotherNode = ClusterStore.connect(testFile2, CLUSTER_NAME, NUM_PARTITIONS);

        node1.assignPartition(0);
        _anotherNode.assignPartition(1);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(node1);
        IOUtils.closeQuietly(_anotherNode);
        FileUtils.tryDelete(testFile1);
        FileUtils.tryDelete(testFile2);
    }

    @Test
    public void append_to_another_node_returns_the_correct_event() {
        String stream = anyStreamForPartition(1, NUM_PARTITIONS);
        var streamName = StreamName.of(stream, 0);
        EventRecord event = EventRecord.create(streamName.name(), "type", Map.of());

        node1.append(event);
        EventRecord found = node1.get(streamName);

        assertEquals(event.stream, found.stream);
        assertEquals(0, found.version);
    }

    @Test
    public void fromStream_of_node_returns_the_correct_event() {
        String stream = anyStreamForPartition(1, NUM_PARTITIONS);
        EventRecord event1 = EventRecord.create(stream, "type", Map.of());
        EventRecord event2 = EventRecord.create(stream, "type", Map.of());

        node1.append(event1);
        node1.append(event2);
        EventLogIterator it = node1.fromStream(StreamName.of(stream));

        assertTrue(it.hasNext());
        EventRecord found = it.next();
        assertEquals(event1.stream, found.stream);
        assertEquals(0, found.version);

        assertTrue(it.hasNext());
        found = it.next();
        assertEquals(event1.stream, found.stream);
        assertEquals(1, found.version);
    }

    @Test
    public void perf() {
        for (int i = 0; i < 5000000; i++) {
            EventRecord event1 = EventRecord.create("stream-" + i, "type", Map.of());
            node1.append(event1);
            if(i % 50000 == 0) {
                System.out.println("-> " + i);
            }
        }
    }

    private static String anyStreamForPartition(int partitionIdx, int numPartitions) {
        int i = 0;
        while (true) {
            var sName = "stream-" + i++;
            long hash = StreamName.hash(sName);
            int idx = (int) (Math.abs(hash) % numPartitions);
            if (idx == partitionIdx) {
                return sName;
            }
        }
    }

}