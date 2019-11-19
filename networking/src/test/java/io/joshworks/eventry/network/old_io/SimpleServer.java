package io.joshworks.eventry.network.old_io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleServer {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(6666);
        OutputStream output = null;
        while (true) {
            try {
                Socket socket = server.accept();
                output = socket.getOutputStream();

                byte[] bytes = new byte[4 * 1024];
                while (true) {
                    output.write(bytes);
                }
            } catch (IOException e) {
                e.printStackTrace();
                try {
                    output.close();
                } catch (Exception e1) {

                }
            }
        }

    }
}
