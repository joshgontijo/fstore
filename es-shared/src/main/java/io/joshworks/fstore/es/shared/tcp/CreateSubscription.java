package io.joshworks.fstore.es.shared.tcp;

public class CreateSubscription {

    public String pattern;

    public CreateSubscription() {
    }

    public CreateSubscription(String pattern) {
        this.pattern = pattern;
    }
}
