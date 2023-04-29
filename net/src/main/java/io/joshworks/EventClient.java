package io.joshworks;

import io.joshworks.handlers.KeepAliveHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class EventClient implements Closeable {

    private final ChannelFuture channel;
    private final EventLoopGroup workerGroup;

    private EventClient(String host, int port, int keepAlive, EventHandler handler) {
        workerGroup = new NioEventLoopGroup();
        channel = connectInternal(host, port, keepAlive, handler, workerGroup);
    }

    public static EventClientBuilder create() {
        return new EventClientBuilder();
    }

    private static ChannelFuture connectInternal(String host, int port, int keepAlive, EventHandler handler, EventLoopGroup workerGroup) {
        try {
            var b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                            .addLast(new ObjectEncoder())
                            .addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())))
                            .addLast(new IdleStateHandler(0, keepAlive, 0, TimeUnit.MILLISECONDS))
                            .addLast(new KeepAliveHandler())
                            .addLast(handler);
                }
            });

            return b.connect(host, port).sync();

        } catch (InterruptedException e) {
            workerGroup.shutdownGracefully();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to connect", e);
        }
    }

    public void send(Object data) {
        channel.channel()
                .writeAndFlush(data);
    }

    @Override
    public void close() {
        channel.channel().close();
        workerGroup.shutdownGracefully();
    }

    public void awaitClose() throws InterruptedException {
        channel.channel().closeFuture().await();
    }

    public static class EventClientBuilder {
        private BiConsumer<ChannelHandlerContext, Object> onMessage = (a, b) -> {
        };
        private BiConsumer<ChannelHandlerContext, Throwable> onError = (a, e) -> e.printStackTrace(System.err);
        private Consumer<ChannelHandlerContext> onConnect = a -> {
        };
        private Consumer<ChannelHandlerContext> onDisconnect = a -> {
        };

        private int keepAlive;


        private EventClientBuilder() {
        }

        public EventClientBuilder onConnect(Consumer<ChannelHandlerContext> onConnect) {
            this.onConnect = requireNonNull(onConnect);
            return this;
        }

        public EventClientBuilder onDisconnect(Consumer<ChannelHandlerContext> onDisconnect) {
            this.onDisconnect = requireNonNull(onDisconnect);
            return this;
        }

        public EventClientBuilder onEvent(BiConsumer<ChannelHandlerContext, Object> onMessage) {
            this.onMessage = requireNonNull(onMessage);
            return this;
        }

        public EventClientBuilder onError(BiConsumer<ChannelHandlerContext, Throwable> onError) {
            this.onError = requireNonNull(onError);
            return this;
        }

        public EventClientBuilder keepAlive(int keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public EventClient connect(String host, int port) {
            return new EventClient(
                    host,
                    port,
                    keepAlive,
                    new EventHandler(
                            onMessage,
                            onError,
                            onConnect,
                            onDisconnect
                    )
            );
        }
    }


}
