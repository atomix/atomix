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
import io.atomix.cluster.MessageSubject;
import io.atomix.cluster.NodeId;
import io.atomix.util.serializer.Serializer;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server protocol dispatcher that uses a {@link ClusterCommunicationService} to communicate.
 */
public class RaftServerMessageDispatcher implements RaftServerProtocolDispatcher {
    private final RaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftServerMessageDispatcher(RaftMessageContext context, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = checkNotNull(context, "context cannot be null");
        this.serializer = checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, NodeId nodeId) {
        return clusterCommunicator.sendAndReceive(request, subject, serializer::encode, serializer::decode, nodeId);
    }

    @Override
    public CompletableFuture<OpenSessionResponse> openSession(NodeId nodeId, OpenSessionRequest request) {
        return sendAndReceive(context.openSessionSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<CloseSessionResponse> closeSession(NodeId nodeId, CloseSessionRequest request) {
        return sendAndReceive(context.closeSessionSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(NodeId nodeId, KeepAliveRequest request) {
        return sendAndReceive(context.keepAliveSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<QueryResponse> query(NodeId nodeId, QueryRequest request) {
        return sendAndReceive(context.querySubject, request, nodeId);
    }

    @Override
    public CompletableFuture<CommandResponse> command(NodeId nodeId, CommandRequest request) {
        return sendAndReceive(context.commandSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(NodeId nodeId, MetadataRequest request) {
        return sendAndReceive(context.metadataSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<JoinResponse> join(NodeId nodeId, JoinRequest request) {
        return sendAndReceive(context.joinSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(NodeId nodeId, LeaveRequest request) {
        return sendAndReceive(context.leaveSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<ConfigureResponse> configure(NodeId nodeId, ConfigureRequest request) {
        return sendAndReceive(context.configureSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<ReconfigureResponse> reconfigure(NodeId nodeId, ReconfigureRequest request) {
        return sendAndReceive(context.reconfigureSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<InstallResponse> install(NodeId nodeId, InstallRequest request) {
        return sendAndReceive(context.installSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<PollResponse> poll(NodeId nodeId, PollRequest request) {
        return sendAndReceive(context.pollSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<VoteResponse> vote(NodeId nodeId, VoteRequest request) {
        return sendAndReceive(context.voteSubject, request, nodeId);
    }

    @Override
    public CompletableFuture<AppendResponse> append(NodeId nodeId, AppendRequest request) {
        return sendAndReceive(context.appendSubject, request, nodeId);
    }

    @Override
    public void publish(NodeId nodeId, PublishRequest request) {
        clusterCommunicator.unicast(request, context.publishSubject(request.session()), serializer::encode, nodeId);
    }
}
