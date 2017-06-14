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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server protocol listener that uses {@link ClusterCommunicationService} for communication.
 */
public class RaftServerMessageListener implements RaftServerProtocolListener {
    private final RaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftServerMessageListener(RaftMessageContext context, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = checkNotNull(context, "context cannot be null");
        this.serializer = checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    @Override
    public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
        clusterCommunicator.addSubscriber(context.openSessionSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterOpenSessionHandler() {
        clusterCommunicator.removeSubscriber(context.openSessionSubject);
    }

    @Override
    public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
        clusterCommunicator.addSubscriber(context.closeSessionSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterCloseSessionHandler() {
        clusterCommunicator.removeSubscriber(context.closeSessionSubject);
    }

    @Override
    public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
        clusterCommunicator.addSubscriber(context.keepAliveSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterKeepAliveHandler() {
        clusterCommunicator.removeSubscriber(context.keepAliveSubject);
    }

    @Override
    public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
        clusterCommunicator.addSubscriber(context.querySubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterQueryHandler() {
        clusterCommunicator.removeSubscriber(context.querySubject);
    }

    @Override
    public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
        clusterCommunicator.addSubscriber(context.commandSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterCommandHandler() {
        clusterCommunicator.removeSubscriber(context.commandSubject);
    }

    @Override
    public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
        clusterCommunicator.addSubscriber(context.metadataSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterMetadataHandler() {
        clusterCommunicator.removeSubscriber(context.metadataSubject);
    }

    @Override
    public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
        clusterCommunicator.addSubscriber(context.joinSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterJoinHandler() {
        clusterCommunicator.removeSubscriber(context.joinSubject);
    }

    @Override
    public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
        clusterCommunicator.addSubscriber(context.leaveSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterLeaveHandler() {
        clusterCommunicator.removeSubscriber(context.leaveSubject);
    }

    @Override
    public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
        clusterCommunicator.addSubscriber(context.configureSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterConfigureHandler() {
        clusterCommunicator.removeSubscriber(context.configureSubject);
    }

    @Override
    public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
        clusterCommunicator.addSubscriber(context.reconfigureSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterReconfigureHandler() {
        clusterCommunicator.removeSubscriber(context.reconfigureSubject);
    }

    @Override
    public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
        clusterCommunicator.addSubscriber(context.installSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterInstallHandler() {
        clusterCommunicator.removeSubscriber(context.installSubject);
    }

    @Override
    public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
        clusterCommunicator.addSubscriber(context.pollSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterPollHandler() {
        clusterCommunicator.removeSubscriber(context.pollSubject);
    }

    @Override
    public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
        clusterCommunicator.addSubscriber(context.voteSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterVoteHandler() {
        clusterCommunicator.removeSubscriber(context.voteSubject);
    }

    @Override
    public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
        clusterCommunicator.addSubscriber(context.appendSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterAppendHandler() {
        clusterCommunicator.removeSubscriber(context.appendSubject);
    }

    @Override
    public void registerResetListener(Consumer<ResetRequest> listener, Executor executor) {
        clusterCommunicator.addSubscriber(context.resetSubject, serializer::decode, listener, executor);
    }

    @Override
    public void unregisterResetListener() {
        clusterCommunicator.removeSubscriber(context.openSessionSubject);
    }
}
