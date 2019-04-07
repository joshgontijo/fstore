package io.joshworks.eventry.network.io;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

class TCPClient {

    private SocketChannel client;

    public void connect(InetSocketAddress address) throws Exception {
        client = SocketChannel.open(address);
    }
}