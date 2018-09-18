package io.joshworks.eventry.server.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class Handler implements Runnable {
    private static final int MAXIN = 4096;
    private static final int MAXOUT = 4096;
    final SocketChannel socket;
    final SelectionKey selectionKey;
    ByteBuffer input = ByteBuffer.allocate(MAXIN);
    ByteBuffer output = ByteBuffer.allocate(MAXOUT);

    static ExecutorService pool = Executors.newFixedThreadPool(2);
    private int read;

    Handler(Selector selector, SocketChannel c) throws IOException {
        socket = c;
        c.configureBlocking(false);
        // Optionally try first read now
        selectionKey = socket.register(selector, 0);
        selectionKey.attach(this);
        selectionKey.interestOps(SelectionKey.OP_READ);
        selector.wakeup();
    }

    boolean inputIsComplete() { //TODO protocol specific
        return read == -1;
    }

    boolean outputIsComplete() { //TODO protocol specific
        return true;
    }

    public void run() { // initial state is reader
        try {
            read = socket.read(input);
            System.out.println("Read " + read);
            input.flip();
            //would end of stream only, for multiple messages: inputIsComplete() || isChunkReceived()
            //and then switch to OP_WRITE inside the Processor
            if (inputIsComplete()) {
                selectionKey.attach(new Sender());
                selectionKey.interestOps(SelectionKey.OP_WRITE);
                pool.execute(new Processor());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    class Processor implements Runnable {

        private synchronized void process() {
            //            CharBuffer charBuffer = input.asCharBuffer();
//            StringBuilder builder = new StringBuilder();
//            while(charBuffer.hasRemaining()) {
//                char c = charBuffer.get();
//                builder.append(c);
//                if(c == '\n') {
//                    System.out.println("READ: " + builder.toString());
//                    builder = new StringBuilder();
//                }
//
//            }

            String value = new String(input.array(), 0, input.limit());
            System.out.println(value);
            input.clear();



        }

        public void run() {
            process();

        }
    }


    class Sender implements Runnable {
        public void run() {
            try {
                socket.write(output);
                output.clear();
                if (outputIsComplete()) {
                    selectionKey.cancel();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
