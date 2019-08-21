/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.joshworks.eventry.network.tcp.conduits;

import org.xnio.Buffers;
import org.xnio.Pooled;
import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.MessageSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;


public final class FramingMessageSourceConduit extends AbstractSourceConduit<StreamSourceConduit> implements MessageSourceConduit {
    public static final int LENGTH_LENGTH = Integer.BYTES;
    private final boolean keepHeader;
    private final Pooled<ByteBuffer> receiveBuffer;
    private boolean ready;

    /**
     * Construct a new instance.
     *
     * @param next          the delegate conduit to set
     * @param receiveBuffer the transmit buffer to use
     */
    public FramingMessageSourceConduit(final StreamSourceConduit next, boolean keepHeader, final Pooled<ByteBuffer> receiveBuffer) {
        super(next);
        this.keepHeader = keepHeader;
        this.receiveBuffer = receiveBuffer;
    }

    @Override
    public void resumeReads() {
        if (ready) next.wakeupReads();
        else next.resumeReads();
    }

    @Override
    public void awaitReadable(final long time, final TimeUnit timeUnit) throws IOException {
        if (!ready) next.awaitReadable(time, timeUnit);
    }

    @Override
    public void awaitReadable() throws IOException {
        if (!ready) next.awaitReadable();
    }

    public void terminateReads() throws IOException {
        receiveBuffer.free();
        next.terminateReads();
    }

    @Override
    public int receive(final ByteBuffer dst) throws IOException {
        final ByteBuffer receiveBuffer = this.receiveBuffer.getResource();
        int res;
        int startPos = receiveBuffer.position();
        do {
            res = next.read(receiveBuffer);
        } while (res > 0);
        int totalRead = receiveBuffer.position() - startPos;
        if (receiveBuffer.position() < LENGTH_LENGTH || (res == -1 && totalRead == 0 && !receiveBuffer.hasRemaining())) {
            if (res == -1) {
                receiveBuffer.clear();
            }
            ready = false;
            return res;
        }

        receiveBuffer.flip();
        try {
            final int length = receiveBuffer.getInt();
            if (length < 0 || length > receiveBuffer.capacity() - LENGTH_LENGTH) {
                Buffers.unget(receiveBuffer, LENGTH_LENGTH);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (receiveBuffer.remaining() < length) {
                if (res == -1) {
                    receiveBuffer.clear();
                } else {
                    Buffers.unget(receiveBuffer, LENGTH_LENGTH);
                }
                ready = false;
                // must be <= 0
                return res;
            }
            if (dst.hasRemaining()) {
                int l = keepHeader ? length - LENGTH_LENGTH : length;
                return Buffers.copy(l, dst, receiveBuffer);
            } else {
                Buffers.skip(receiveBuffer, length);
                return 0;
            }
        } finally {
            if (res != -1) {
                receiveBuffer.compact();
                if (receiveBuffer.position() >= LENGTH_LENGTH && receiveBuffer.position() >= (LENGTH_LENGTH + receiveBuffer.getInt(0))) {
                    // there's another packet ready to go
                    ready = true;
                }
            }
        }

    }

    @Override
    public long receive(final ByteBuffer[] dsts, final int offs, final int len) throws IOException {
        final ByteBuffer receiveBuffer = this.receiveBuffer.getResource();
        int res;
        do {
            res = next.read(receiveBuffer);
        } while (res > 0);
        if (receiveBuffer.position() < 4) {
            if (res == -1) {
                receiveBuffer.clear();
            }
            ready = false;
            return res;
        }
        receiveBuffer.flip();
        try {
            final int length = receiveBuffer.getInt();
            if (length < 0 || length > receiveBuffer.capacity() - 4) {
                Buffers.unget(receiveBuffer, 4);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (receiveBuffer.remaining() < length) {
                if (res == -1) {
                    receiveBuffer.clear();
                } else {
                    Buffers.unget(receiveBuffer, 4);
                }
                ready = false;
                // must be <= 0
                return res;
            }
            if (Buffers.hasRemaining(dsts, offs, len)) {
                int l = keepHeader ? length - 4 : length;
                return Buffers.copy(l, dsts, offs, len, receiveBuffer);
            } else {
                Buffers.skip(receiveBuffer, length);
                return 0;
            }
        } finally {
            if (res != -1) {
                receiveBuffer.compact();
                if (receiveBuffer.position() >= 4 && receiveBuffer.position() >= 4 + receiveBuffer.getInt(0)) {
                    // there's another packet ready to go
                    ready = true;
                }
            }
        }
    }
}
