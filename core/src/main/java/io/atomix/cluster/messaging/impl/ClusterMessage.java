/*
 * Copyright 2014-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.utils.ArraySizeHashPrinter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Base message for cluster-wide communications.
 */
public class ClusterMessage {

    private final NodeId sender;
    private final MessageSubject subject;
    private final byte[] payload;
    private transient byte[] response;

    /**
     * Creates a cluster message.
     *
     * @param sender  message sender
     * @param subject message subject
     * @param payload message payload
     */
    public ClusterMessage(NodeId sender, MessageSubject subject, byte[] payload) {
        this.sender = sender;
        this.subject = subject;
        this.payload = payload;
    }

    /**
     * Returns the id of the controller sending this message.
     *
     * @return message sender id.
     */
     public NodeId sender() {
         return sender;
     }

    /**
     * Returns the message subject indicator.
     *
     * @return message subject
     */
    public MessageSubject subject() {
        return subject;
    }

    /**
     * Returns the message payload.
     *
     * @return message payload.
     */
    public byte[] payload() {
        return payload;
    }

    /**
     * Records the response to be sent to the sender.
     *
     * @param data response payload
     */
    public void respond(byte[] data) {
        response = data;
    }

    /**
     * Returns the response to be sent to the sender.
     *
     * @return response bytes
     */
    public byte[] response() {
        return response;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("sender", sender)
                .add("subject", subject)
                .add("payload", ArraySizeHashPrinter.of(payload))
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ClusterMessage)) {
            return false;
        }

        ClusterMessage that = (ClusterMessage) o;

        return Objects.equals(this.sender, that.sender) &&
                Objects.equals(this.subject, that.subject) &&
                Arrays.equals(this.payload, that.payload);
    }

    /**
     * Serializes this instance.
     * @return bytes
     */
    public byte[] getBytes() {
        byte[] senderBytes = sender.toString().getBytes(Charsets.UTF_8);
        byte[] subjectBytes = subject.name().getBytes(Charsets.UTF_8);
        int capacity = 12 + senderBytes.length + subjectBytes.length + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(senderBytes.length);
        buffer.put(senderBytes);
        buffer.putInt(subjectBytes.length);
        buffer.put(subjectBytes);
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    /**
     * Decodes a new ClusterMessage from raw bytes.
     * @param bytes raw bytes
     * @return cluster message
     */
    public static ClusterMessage fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] senderBytes = new byte[buffer.getInt()];
        buffer.get(senderBytes);
        byte[] subjectBytes = new byte[buffer.getInt()];
        buffer.get(subjectBytes);
        byte[] payloadBytes = new byte[buffer.getInt()];
        buffer.get(payloadBytes);

        return new ClusterMessage(new NodeId(new String(senderBytes, Charsets.UTF_8)),
                new MessageSubject(new String(subjectBytes, Charsets.UTF_8)),
                payloadBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sender, subject, payload);
    }
}
