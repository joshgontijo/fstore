package io.joshworks.fstore.log.server.test_app;

import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TestApp {

    private static final LogAppender<Event> treq = LogAppender.builder(new File("S:\\stream-test\\transactions-requests"), JsonSerializer.of(Event.class)).open();
    private static final LogAppender<Event> accounts = LogAppender.builder(new File("S:\\stream-test\\accounts"), JsonSerializer.of(Event.class)).open();
    private static final LogAppender<Event> transactions = LogAppender.builder(new File("S:\\stream-test\\transactions"), JsonSerializer.of(Event.class)).open();

    public static void main(String[] args) {

        treq.append(new Tra)


    }

    private static class Consumer {

        private final LogIterator<Event> iterator = transactions.iterator(Direction.FORWARD);
        private final Map<String, Account> accounts = new HashMap<>();

        public Consumer() {
            new Thread(() -> {
                while (true) {
                    while (iterator.hasNext()) {
                        Event event = iterator.next();
                        processEvent(event);
                    }
                    Threads.sleep(1000);
                }
            }).start();
        }

        private void processEvent(Event event) {
            if (event.data instanceof AccountCreated) {
                AccountCreated data = (AccountCreated) event.data;
                accounts.put(data.id, new Account(data.id, 0));
            }
            if (event.data instanceof MoneyDeposited) {
                MoneyDeposited data = (MoneyDeposited) event.data;
                accounts.get(data.account).amount += data.amount;
            }
            if (event.data instanceof MoneyWithdraw) {
                MoneyWithdraw data = (MoneyWithdraw) event.data;
                accounts.get(data.account).amount -= data.amount;
            }
            if (event.data instanceof MoneyTransferred) {
                MoneyTransferred data = (MoneyTransferred) event.data;
                accounts.get(data.fromAccount).amount -= data.amount;
                accounts.get(data.toAccount).amount += data.amount;
            }
        }


    }

    private static class MoneyWithdraw {

        private final String account;
        private final int amount;

        private MoneyWithdraw(String account, int amount) {
            this.account = account;
            this.amount = amount;
        }
    }

    private static class MoneyDeposited {

        private final String account;
        private final int amount;

        private MoneyDeposited(String account, int amount) {
            this.account = account;
            this.amount = amount;
        }
    }

    private static class MoneyTransferred {

        private final String fromAccount;
        private final String toAccount;
        private final int amount;


        private MoneyTransferred(String fromAccount, String toAccount, int amount) {
            this.fromAccount = fromAccount;
            this.toAccount = toAccount;
            this.amount = amount;
        }
    }

    private static class AccountCreated {
        private final String id;

        private AccountCreated(String id) {
            this.id = id;
        }
    }

    private static class Account {
        private final String id;
        private int amount;

        private Account(String id, int amount) {
            this.id = id;
            this.amount = amount;
        }
    }

    public static class Event {
        private final long sequence;
        private final String type;
        private final Object data;

        public Event(long sequence, String type, Object data) {
            this.sequence = sequence;
            this.type = type;
            this.data = data;
        }
    }

}
