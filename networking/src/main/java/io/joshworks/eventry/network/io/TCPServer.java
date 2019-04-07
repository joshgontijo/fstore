package io.joshworks.eventry.network.io;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.Socket;

class TCPServer {

    private Socket connectionSocket;

    private void start(int port) throws Exception {
        ServerSocket welcomeSocket = new ServerSocket(port);

        byte[] buffer = new byte[4096];
        while (true) {
            connectionSocket = welcomeSocket.accept();
            DataInputStream  in = new DataInputStream(connectionSocket.getInputStream());
            int read = in.read(buffer);

        }

    }
}