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

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.buffers.StupidPool;
import org.xnio.conduits.AbstractSourceConduit;
import org.xnio.conduits.MessageSourceConduit;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;


public final class FramingMessageSourceConduit extends AbstractSourceConduit<StreamSourceConduit> implements MessageSourceConduit {
    public static final int LENGTH_LENGTH = Integer.BYTES;
    private final int maxMessageSize;
    private final StupidPool pool;
    private ByteBuffer frameBuffer;
    private boolean ready;

    /**
     * Construct a new instance.
     *
     * @param next the delegate conduit to set
     * @param pool the transmit buffer to use
     */
    public FramingMessageSourceConduit(final StreamSourceConduit next, int maxMessageSize, final StupidPool pool) {
        super(next);
        this.maxMessageSize = maxMessageSize;
        this.pool = pool;
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
        releaseBuffer();
        next.terminateReads();
    }

    private void releaseBuffer() {
        pool.free(frameBuffer);
        frameBuffer = null;
    }

    @Override
    public int receive(final ByteBuffer dst) throws IOException {
        frameBuffer = frameBuffer == null ? pool.allocate() : frameBuffer;
        int res;
        do {
            res = next.read(frameBuffer);
        } while (res > 0);
        if (frameBuffer.position() < LENGTH_LENGTH) {
            if (res == -1) {
                frameBuffer.clear();
            }
            ready = false;
            return res;
        }

        frameBuffer.flip();
        try {
            final int length = frameBuffer.getInt();
            if (length < 0 || length > frameBuffer.capacity() - LENGTH_LENGTH) {
                Buffers.offsetPosition(frameBuffer, -LENGTH_LENGTH);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (length > maxMessageSize) {
                throw new IllegalStateException("Message too large: " + length + ", max allowed: " + maxMessageSize);
            }
            if (frameBuffer.remaining() < length) {
                if (res == -1) {
                    frameBuffer.clear();
                } else {
                    Buffers.offsetPosition(frameBuffer, -LENGTH_LENGTH);
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
                if (frameBuffer.position() >= LENGTH_LENGTH && frameBuffer.position() >= (LENGTH_LENGTH + frameBuffer.getInt(0))) {
                    // there's another packet ready to go
                    ready = true;
                }
            }
            //buffer has no data left, release it
            if (frameBuffer.position() == 0) {
                releaseBuffer();
            }
        }
    }

    @Override
    public long receive(final ByteBuffer[] dsts, final int offs, final int len) throws IOException {
        final ByteBuffer receiveBuffer = this.pool.allocate();
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
                Buffers.offsetPosition(receiveBuffer, -4);
                throw new IllegalStateException("Invalid message length: " + length);
            }
            if (receiveBuffer.remaining() < length) {
                if (res == -1) {
                    receiveBuffer.clear();
                } else {
                    Buffers.offsetPosition(receiveBuffer, -4);
                }
                ready = false;
                // must be <= 0
                return res;
            }
            if (Buffers.hasRemaining(dsts, offs, len)) {
                return org.xnio.Buffers.copy(length, dsts, offs, len, receiveBuffer);
            } else {
                Buffers.offsetPosition(receiveBuffer, length);
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
