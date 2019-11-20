package io.joshworks.fstore.server;

import io.joshworks.snappy.websocket.WebsocketEndpoint;
import io.undertow.websockets.core.BufferedBinaryMessage;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.xnio.Pooled;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WsEndpoint extends WebsocketEndpoint {

    @Override
    public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {

    }

    @Override
    protected void onFullBinaryMessage(WebSocketChannel channel, BufferedBinaryMessage message) throws IOException {
        Pooled<ByteBuffer[]> pulledData = message.getData();
        WebSockets.mergeBuffers(pulledData.getResource());
    }

    @Override
    protected void onClose(WebSocketChannel webSocketChannel, StreamSourceFrameChannel channel) throws IOException {
        System.out.println("Connection closed");
    }
}
