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
package io.atomix.protocols.raft.messaging;

import io.atomix.cluster.ClusterCommunicationService;
import io.atomix.cluster.MessageSubject;
import io.atomix.cluster.NodeId;
import io.atomix.util.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client protocol dispatcher that uses a cluster communicator to dispatch messages.
 */
public class RaftClientMessageDispatcher implements RaftClientProtocolDispatcher {
    private final RaftClientCommunicator.MessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftClientMessageDispatcher(RaftClientCommunicator.MessageContext context, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = checkNotNull(context, "context cannot be null");
        this.serializer = checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    private static MessageSubject getSubject(String prefix, String type) {
        if (prefix == null) {
            return new MessageSubject(type);
        } else {
            return new MessageSubject(String.format("%s-%s", prefix, type));
        }
    }

    private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, NodeId nodeId) {
        return clusterCommunicator.sendAndReceive(request, subject, serializer::encode, serializer::decode, nodeId);
    }

    @Override
    public CompletableFuture<OpenSessionReply> openSession(NodeId nodeId, OpenSessionMessage request) {
        return sendAndReceive(context.openSessionSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<CloseSessionReply> closeSession(NodeId nodeId, CloseSessionMessage request) {
        return sendAndReceive(context.closeSessionSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<KeepAliveReply> keepAlive(NodeId nodeId, KeepAliveMessage request) {
        return sendAndReceive(context.keepAliveSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<QueryReply> query(NodeId nodeId, QueryMessage request) {
        return sendAndReceive(context.querySubject, request, nodeId);
    }

    @Override
    public CompletableFuture<CommandReply> command(NodeId nodeId, CommandMessage request) {
        return sendAndReceive(context.commandSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<MetadataReply> metadata(NodeId nodeId, MetadataMessage request) {
        return sendAndReceive(context.metadataSubject, request, nodeId);
    }

    @Override
    public void reset(ResetMessage request) {
        clusterCommunicator.broadcast(request, context.resetSubject, serializer::encode);
    }
}
