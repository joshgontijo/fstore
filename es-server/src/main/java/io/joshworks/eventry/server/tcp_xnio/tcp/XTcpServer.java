package io.joshworks.eventry.server.tcp_xnio.tcp;

import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.AbstractStreamSinkConduit;
import org.xnio.conduits.StreamSinkConduit;

import java.io.IOException;
import java.nio.ByteBuffer;

public class XTcpServer {

    private final XnioWorker worker;
    private final AcceptingChannel<StreamConnection> server;

    public static void main(String[] args) {

        OptionMap options = OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, 5)
                .set(Options.WORKER_TASK_CORE_THREADS, 3)
                .set(Options.WORKER_TASK_MAX_THREADS, 3)
                .set(Options.WORKER_NAME, "josh-worker")
                .set(Options.KEEP_ALIVE, true)
                .getMap();

//        new XTcpServer(options);


    }

    private XTcpServer(XnioWorker worker, AcceptingChannel<StreamConnection> server) {
        this.worker = worker;
        this.server = server;
    }

    private static class LoggingConduit extends AbstractStreamSinkConduit<StreamSinkConduit> {

        protected LoggingConduit(StreamSinkConduit next) {
            super(next);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            System.out.println("Writing: " + Thread.currentThread().getName());
            return super.write(src);
        }
    }

}
