package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.core.util.TestUtils;
import org.xnio.ChannelListener;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

public class TestServer {

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, NoSuchProviderException, KeyManagementException, InterruptedException {

        final StupidPool pool = new StupidPool(50, 4096);


        Xnio xnio = Xnio.getInstance();
        XnioWorker worker = xnio.createWorker(OptionMap.EMPTY);


        FileChannel fileChannel = FileChannel.open(TestUtils.testFile().toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        AcceptingChannel<StreamConnection> acceptingChannel = worker.createStreamConnectionServer(new InetSocketAddress("localhost", 9999), new ChannelListener<AcceptingChannel<StreamConnection>>() {
            @Override
            public void handleEvent(AcceptingChannel<StreamConnection> channel) {
                StreamConnection conn = null;
                try {
                    while ((conn = channel.accept()) != null) {
                        System.out.println("Accepted connection");
                        conn.getSourceChannel().setReadListener(new ChannelListener<>() {

                            long written = 0;
                            int writes = 0;

                            @Override
                            public void handleEvent(ConduitStreamSourceChannel channel) {
                                ByteBuffer b = pool.allocate();
                                try {
//                                    if (channel.read(b) == -1) {
//                                        return;
//                                    }

//                                    b.flip();
//                                    written += b.remaining();
                                    fileChannel.write(b);
                                    long write = channel.transferTo(written, Long.MAX_VALUE, fileChannel);
                                    written = write >= 0 ? written + write : written;

                                        fileChannel.force(false);
//                                    if (written %  >= 100000) {
//                                        writes = 0;
//                                    }

                                } catch (IOException e) {
                                    e.printStackTrace();
                                } finally {
                                    pool.free(b);
                                }
                            }

                        });
                        conn.getSourceChannel().resumeReads();
                        conn.getSinkChannel().resumeWrites();
                    }

                } catch (Exception e) {
                    IOUtils.closeQuietly(conn);
                    throw new RuntimeException(e);
                }

            }
        }, OptionMap.EMPTY);

        acceptingChannel.resumeAccepts();

        worker.awaitTermination();

    }

}
