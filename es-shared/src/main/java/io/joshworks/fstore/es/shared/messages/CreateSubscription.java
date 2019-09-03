package io.joshworks.fstore.es.shared.messages;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CreateSubscription {

    public Set<String> pattern;

    public CreateSubscription() {
    }

    public CreateSubscription(String... patterns) {
        this.pattern = new HashSet<>(Arrays.asList(patterns));
    }
}
