/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClockTimestamp;

/**
 * Represents a single instance of a subscription.
 */
public class Subscription {
    private final NodeId nodeId;
    private final MessageSubject subject;
    private final LogicalTimestamp logicalTimestamp;
    private final boolean tombstone;
    private final WallClockTimestamp timestamp = new WallClockTimestamp();

    Subscription(NodeId nodeId, MessageSubject subject, LogicalTimestamp logicalTimestamp) {
        this(nodeId, subject, logicalTimestamp, false);
    }

    private Subscription(
            NodeId nodeId,
            MessageSubject subject,
            LogicalTimestamp logicalTimestamp,
            boolean tombstone) {
        this.nodeId = nodeId;
        this.subject = subject;
        this.logicalTimestamp = logicalTimestamp;
        this.tombstone = tombstone;
    }

    /**
     * Returns the subscription node identifier.
     *
     * @return the subscription node identifier
     */
    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * Returns the subscription subject.
     *
     * @return the subscription subject
     */
    public MessageSubject subject() {
        return subject;
    }

    /**
     * Returns the logical subscription timestamp.
     *
     * @return the logical subscription timestamp
     */
    LogicalTimestamp logicalTimestamp() {
        return logicalTimestamp;
    }

    /**
     * Returns a boolean indicating whether the subscription is a tombstone.
     *
     * @return indicates whether the subscription is a tombstone
     */
    boolean isTombstone() {
        return tombstone;
    }

    /**
     * Returns the time at which the subscription was created.
     *
     * @return the time at which the subscription was created
     */
    public WallClockTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns the subscription as a tombstone.
     *
     * @return the subscription as a tombstone
     */
    Subscription asTombstone() {
        return new Subscription(nodeId, subject, logicalTimestamp, true);
    }
}