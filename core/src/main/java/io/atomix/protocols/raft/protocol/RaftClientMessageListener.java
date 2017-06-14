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
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.ClusterCommunicationService;
import io.atomix.util.serializer.Serializer;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client listener that uses a cluster communicator for messaging.
 */
public class RaftClientMessageListener implements RaftClientProtocolListener {
    private final RaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftClientMessageListener(RaftMessageContext context, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = checkNotNull(context, "context cannot be null");
        this.serializer = checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    @Override
    public void registerPublishListener(long sessionId, Consumer<PublishRequest> listener, Executor executor) {
        clusterCommunicator.addSubscriber(context.publishSubject(sessionId), serializer::decode, listener, executor);
    }

    @Override
    public void unregisterPublishListener(long sessionId) {
        clusterCommunicator.removeSubscriber(context.publishSubject(sessionId));
    }
}
