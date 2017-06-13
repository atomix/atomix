/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.messaging;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client operation request.
 * <p>
 * Operation requests are sent by clients to servers to execute operations on the replicated state
 * machine. Each operation request must be sequenced with a {@link #sequence()} number. All operations
 * will be applied to replicated state machines in the sequence in which they were sent by the client.
 * Sequence numbers must always be sequential, and in the event that an operation request fails, it must
 * be resent by the client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class OperationMessage extends SessionMessage {
    protected final long sequence;
    protected final byte[] bytes;

    protected OperationMessage(long session, long sequence, byte[] bytes) {
        super(session);
        this.sequence = sequence;
        this.bytes = bytes;
    }

    /**
     * Returns the request sequence number.
     *
     * @return The request sequence number.
     */
    public long sequence() {
        return sequence;
    }

    /**
     * Returns the request operation.
     *
     * @return The request operation.
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * Operation request builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends OperationMessage> extends SessionMessage.Builder<T, U> {
        protected long sequence;
        protected byte[] bytes;

        /**
         * Sets the request sequence number.
         *
         * @param sequence The request sequence number.
         * @return The request builder.
         * @throws IllegalArgumentException If the request sequence number is not positive.
         */
        @SuppressWarnings("unchecked")
        public T withSequence(long sequence) {
            checkArgument(sequence >= 0, "sequence must be positive");
            this.sequence = sequence;
            return (T) this;
        }

        /**
         * Sets the request bytes.
         *
         * @param bytes The request operation bytes.
         * @return The request builder.
         * @throws NullPointerException if the request {@code bytes} are {@code null}
         */
        @SuppressWarnings("unchecked")
        public T withBytes(byte[] bytes) {
            this.bytes = checkNotNull(bytes, "bytes cannot be null");
            return (T) this;
        }

        @Override
        protected void validate() {
            super.validate();
            checkNotNull(bytes, "bytes cannot be null");
            checkArgument(sequence >= 0, "sequence must be positive");
        }
    }
}
