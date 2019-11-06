package io.joshworks.eventry.network.tcp.conduits;

import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

public class DelegateSourceConduit extends AbstractSourceConduit<StreamSourceConduit> {

    protected DelegateSourceConduit(StreamSourceConduit next) {
        super(next);
    }
}