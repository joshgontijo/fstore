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

package io.joshworks.fstore.tcp.conduits;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.MessageSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.joshworks.fstore.tcp.TcpHeader.RECORD_LEN_LENGTH;


public final class FramingMessageSourceConduit extends AbstractSourceConduit<StreamSourceConduit> implements MessageSourceConduit {

    private final ByteBuffer frameBuffer;
    private final BufferPool pool;
    private boolean ready;

    /**
     * Construct a new instance.
     *
     * @param next the delegate conduit to set
     * @param pool the transmit buffer to use
     */
    public FramingMessageSourceConduit(final StreamSourceConduit next, BufferPool pool) {
        super(next);
        this.pool = pool;
        this.frameBuffer = pool.allocate();
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

    @Override
    public void terminateReads() throws IOException {
        pool.free(frameBuffer);
        next.terminateReads();
    }


    @Override
    public int receive(final ByteBuffer dst) throws IOException {
        int res;
        do {
            res = next.read(frameBuffer);
        } while (res > 0);
        if (frameBuffer.position() < RECORD_LEN_LENGTH) {
            if (res == -1) {
                frameBuffer.clear();
            }
            ready = false;
            return res;
        }

        frameBuffer.flip();
        try {
            final int length = frameBuffer.getInt();
            if (length < 0 || length > frameBuffer.capacity() - RECORD_LEN_LENGTH) {
                Buffers.offsetPosition(frameBuffer, -RECORD_LEN_LENGTH);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (frameBuffer.remaining() < length) {
                if (res == -1) {
                    frameBuffer.clear();
                } else {
                    Buffers.offsetPosition(frameBuffer, -RECORD_LEN_LENGTH);
                }
                ready = false;
                // must be <= 0
                return res;
            }
            if (dst.hasRemaining()) {
                int copied = Buffers.copy(frameBuffer, frameBuffer.position(), length, dst);
                Buffers.offsetPosition(frameBuffer, copied);
                return copied;
            } else {
                Buffers.offsetPosition(frameBuffer, length);
                return 0;
            }
        } finally {
            if (res != -1) {
                frameBuffer.compact();
                if (frameBuffer.position() >= RECORD_LEN_LENGTH && frameBuffer.position() >= (RECORD_LEN_LENGTH + frameBuffer.getInt(0))) {
                    // there's another packet ready to go
                    ready = true;
                }
            }
        }
    }

    @Override
    public long receive(final ByteBuffer[] dsts, final int offs, final int len) throws IOException {
        int res;
        do {
            res = next.read(frameBuffer);
        } while (res > 0);
        if (frameBuffer.position() < RECORD_LEN_LENGTH) {
            if (res == -1) {
                frameBuffer.clear();
            }
            ready = false;
            return res;
        }
        frameBuffer.flip();
        try {
            final int length = frameBuffer.getInt();
            if (length < 0 || length > frameBuffer.capacity() - RECORD_LEN_LENGTH) {
                Buffers.offsetPosition(frameBuffer, -RECORD_LEN_LENGTH);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (frameBuffer.remaining() < length) {
                if (res == -1) {
                    frameBuffer.clear();
                } else {
                    Buffers.offsetPosition(frameBuffer, -RECORD_LEN_LENGTH);
                }
                ready = false;
                // must be <= 0
                return res;
            }
            if (Buffers.hasRemaining(dsts, offs, len)) {
                return org.xnio.Buffers.copy(length, dsts, offs, len, frameBuffer);
            } else {
                Buffers.offsetPosition(frameBuffer, length);
                return 0;
            }
        } finally {
            if (res != -1) {
                frameBuffer.compact();
                if (frameBuffer.position() >= RECORD_LEN_LENGTH && frameBuffer.position() >= RECORD_LEN_LENGTH + frameBuffer.getInt(0)) {
                    // there's another packet ready to go
                    ready = true;
                }
            }
        }
    }
}
