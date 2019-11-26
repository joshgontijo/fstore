package io.joshworks.lsm.server;

import io.joshworks.lsm.server.client.Client;

import java.net.InetSocketAddress;


public class TestClient {

    public static void main(String[] args) {

        try (Client client = Client.connect(new InetSocketAddress("localhost", 1000))) {
            client.put("123", new User("Hello", 123));
        }

        try (Client client = Client.connect(new InetSocketAddress("localhost", 2000))) {
            client.put("123", new User("Hello", 123));
        }


    }

    public static class User {
        public String name;
        public int age;

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

}
