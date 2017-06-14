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
package io.atomix.protocols.raft.session.impl;

import io.atomix.cluster.NodeId;
import io.atomix.messaging.MessagingException;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocolDispatcher;
import io.atomix.protocols.raft.protocol.RaftRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftConnection.class);

    private final String name;
    private final RaftClientProtocolDispatcher dispatcher;
    private final NodeSelector selector;
    private NodeId node;
    private volatile boolean open;

    public RaftConnection(String name, RaftClientProtocolDispatcher dispatcher, NodeSelector selector) {
        this.name = checkNotNull(name, "name cannot be null");
        this.dispatcher = checkNotNull(dispatcher, "dispatcher cannot be null");
        this.selector = checkNotNull(selector, "selector cannot be null");
    }

    /**
     * Returns the current selector leader.
     *
     * @return The current selector leader.
     */
    public NodeId leader() {
        return selector.leader();
    }

    /**
     * Returns the current set of servers.
     *
     * @return The current set of servers.
     */
    public Collection<NodeId> servers() {
        return selector.servers();
    }

    /**
     * Resets the client connection.
     *
     * @return The client connection.
     */
    public RaftConnection reset() {
        selector.reset();
        return this;
    }

    /**
     * Resets the client connection.
     *
     * @param leader  The current cluster leader.
     * @param servers The current servers.
     * @return The client connection.
     */
    public RaftConnection reset(NodeId leader, Collection<NodeId> servers) {
        selector.reset(leader, servers);
        return this;
    }

    /**
     * Sends an open session request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
        CompletableFuture<OpenSessionResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::openSession, future);
        return future;
    }

    /**
     * Sends a close session request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
        CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::closeSession, future);
        return future;
    }

    /**
     * Sends a keep alive request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::keepAlive, future);
        return future;
    }

    /**
     * Sends a query request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<QueryResponse> query(QueryRequest request) {
        CompletableFuture<QueryResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::query, future);
        return future;
    }

    /**
     * Sends a command request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<CommandResponse> command(CommandRequest request) {
        CompletableFuture<CommandResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::command, future);
        return future;
    }

    /**
     * Sends a metadata request to the given node.
     *
     * @param request the request to send
     * @return a future to be completed with the response
     */
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
        sendRequest(request, dispatcher::metadata, future);
        return future;
    }

    /**
     * Sends the given request attempt to the cluster.
     */
    protected <T extends RaftRequest, U extends RaftResponse> void sendRequest(T request, BiFunction<NodeId, T, CompletableFuture<U>> sender, CompletableFuture<U> future) {
        if (open) {
            NodeId node = next();
            if (node != null) {
                LOGGER.trace("{} - Sending {}", name, request);
                sender.apply(node, request).whenComplete((r, e) -> {
                    if (e != null || r != null) {
                        handleResponse(request, sender, node, r, e, future);
                    } else {
                        future.complete(null);
                    }
                });
            } else {
                future.completeExceptionally(new ConnectException("Failed to connect to the cluster"));
            }
        }
    }

    /**
     * Resends a request due to a request failure, resetting the connection if necessary.
     */
    @SuppressWarnings("unchecked")
    protected <T extends RaftRequest> void resendRequest(Throwable cause, T request, BiFunction sender, NodeId node, CompletableFuture future) {
        // If the connection has not changed, reset it and connect to the next server.
        if (this.node == node) {
            LOGGER.trace("{} - Resetting connection. Reason: {}", name, cause);
            this.node = null;
        }

        // Attempt to send the request again.
        sendRequest(request, sender, future);
    }

    /**
     * Handles a response from the cluster.
     */
    @SuppressWarnings("unchecked")
    protected <T extends RaftRequest> void handleResponse(T request, BiFunction sender, NodeId node, RaftResponse response, Throwable error, CompletableFuture future) {
        if (error == null) {
            if (response.status() == RaftResponse.Status.OK
                    || response.error() == RaftError.Type.COMMAND_ERROR
                    || response.error() == RaftError.Type.QUERY_ERROR
                    || response.error() == RaftError.Type.APPLICATION_ERROR
                    || response.error() == RaftError.Type.UNKNOWN_CLIENT_ERROR
                    || response.error() == RaftError.Type.UNKNOWN_SESSION_ERROR
                    || response.error() == RaftError.Type.UNKNOWN_STATE_MACHINE_ERROR
                    || response.error() == RaftError.Type.INTERNAL_ERROR) {
                LOGGER.trace("{} - Received {}", name, response);
                future.complete(response);
            } else {
                resendRequest(response.error().createException(), request, sender, node, future);
            }
        } else if (error instanceof ConnectException || error instanceof TimeoutException || error instanceof MessagingException || error instanceof ClosedChannelException) {
            resendRequest(error, request, sender, node, future);
        } else {
            LOGGER.debug("{} - {} failed! Reason: {}", name, request, error);
            future.completeExceptionally(error);
        }
    }

    /**
     * Connects to the cluster.
     */
    protected NodeId next() {
        // If the address selector has been reset then reset the connection.
        if (selector.state() == NodeSelector.State.RESET && node != null) {
            this.node = selector.next();
            return this.node;
        }

        // If a connection was already established then use that connection.
        if (node != null) {
            return node;
        }

        if (!selector.hasNext()) {
            LOGGER.debug("{} - Failed to connect to the cluster", name);
            reset();
            return null;
        } else {
            this.node = selector.next();
            LOGGER.debug("{} - Connecting to {}", name, node);
            return node;
        }
    }

}
