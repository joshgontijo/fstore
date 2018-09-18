package io.joshworks.eventry.server.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;


public class NioServer implements Runnable {


    public static void main(String[] args) throws IOException {
        new NioServer(9090).run();
    }

    final Selector selector;
    final ServerSocketChannel serverSocket;

    NioServer(int port) throws IOException {
        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();
        serverSocket.socket().bind(new InetSocketAddress(port));
        serverSocket.configureBlocking(false);
        SelectionKey sk = serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        sk.attach(new Acceptor());
    }
        /*
        Alternatively, use explicit SPI provider:
        SelectorProvider p = SelectorProvider.provider();
        selector = p.openSelector();
        serverSocket = p.openServerSocketChannel();
        */

    // class Reactor continued
    public void run() { // normally in a new Thread
        try {
            while (!Thread.interrupted()) {
                selector.select();
                Set<SelectionKey> selected = selector.selectedKeys();
                Iterator<SelectionKey> it = selected.iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    System.out.println("A: " + key.isAcceptable() + " R: " + key.isReadable() + " W: " + key.isWritable());
                    dispatch(key);
                    it.remove();
                }
                selected.clear();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void dispatch(SelectionKey k) {
        Runnable r = (Runnable) (k.attachment());
        if (r != null)
            r.run();
    }

    // class Reactor continued
    class Acceptor implements Runnable { // inner
        public void run() {
            try {
                SocketChannel c = serverSocket.accept();
                if (c != null)
                    new Handler(selector, c);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


    //Acceptor2
    Selector[] selectors; // also create threads
    int next = 0;
    class Acceptor2 { // ...
        public synchronized void run() {
            try {
                SocketChannel connection = serverSocket.accept();
                if (connection != null)
                    new Handler(selectors[next], connection);
                if (++next == selectors.length) next = 0;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}

