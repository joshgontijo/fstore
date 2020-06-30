package io.joshworks.fstore.test;

import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.Lsm;
import io.joshworks.net.tcp.TcpServer;

import java.io.File;

public class TestServer {

    public static void main(String[] args) {

        File root = TestUtils.testFolder();

        File master = new File(root, "master");
        Lsm lsmMaster = Lsm.create(master, RowKey.LONG)
                .compactionThreshold(-1)
                .open();




        TcpServer.builder()
                .



        File replica = new File(root, "replica");
        Lsm lsmReplica = Lsm.create(master, RowKey.LONG)
                .compactionThreshold(-1)
                .open();



    }

}
