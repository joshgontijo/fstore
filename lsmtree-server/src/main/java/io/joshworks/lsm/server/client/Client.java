package io.joshworks.lsm.server.client;

import io.joshworks.eventry.network.tcp.TcpClientConnection;
import io.joshworks.eventry.network.tcp.client.TcpEventClient;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.lsm.server.messages.Ack;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class Client {

    private final TcpClientConnection connection;

    private Client(TcpClientConnection connection) {
        this.connection = connection;
    }

    public static Client connect(InetSocketAddress address) {
        TcpClientConnection clientConnection = TcpEventClient.create()
//                .keepAlive(2, TimeUnit.SECONDS)
//                .option(Options.SEND_BUFFER, Size.MB.ofInt(100))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .bufferSize(Size.KB.ofInt(16))
                .option(Options.SEND_BUFFER, Size.KB.ofInt(4))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(4))
                .onEvent((connection, data) -> {
                    //TODO
                })
                .connect(address, 5, TimeUnit.SECONDS);

        return new Client(clientConnection);
    }

    public void put(String key, Object value) {
        Response<Ack> request = connection.request(new Put(key, JsonSerializer.toBytes(value)));
        request.get();
    }

    public <T> T get(String key, Class<T> type) {
        Response<Result> request = connection.request(new Get(key));
        Result result = request.get();
        return JsonSerializer.fromJson(result.value, type);
    }

    public void delete(String key) {
        Response<Ack> request = connection.request(new Delete(key));
        request.get();
    }

}