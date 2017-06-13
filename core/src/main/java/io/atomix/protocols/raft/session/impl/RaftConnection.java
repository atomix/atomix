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

import io.atomix.cluster.ClusterCommunicationService;
import io.atomix.cluster.NodeId;
import io.atomix.messaging.MessagingException;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.protocol.RaftRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.util.serializer.KryoNamespaces;
import io.atomix.util.serializer.Serializer;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RaftConnection {
    protected static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.RAFT);

    protected final String clusterName;
    protected final ClusterCommunicationService communicationService;
    protected final NodeSelector selector;
    protected final Map<String, Function> handlers = new ConcurrentHashMap<>();
    private NodeId node;
    protected volatile boolean open;

    public RaftConnection(String clusterName, ClusterCommunicationService communicationService, NodeSelector selector) {
        this.clusterName = checkNotNull(clusterName, "clusterName cannot be null");
        this.communicationService = checkNotNull(communicationService, "communicationService cannot be null");
        this.selector = checkNotNull(selector, "selector cannot be null");
    }

    /**
     * Returns the connection name.
     *
     * @return The connection name.
     */
    protected abstract String name();

    /**
     * Returns the connection logger.
     *
     * @return The connection logger.
     */
    protected abstract Logger logger();

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
     * Sends a request to the cluster.
     *
     * @param request The request to send.
     * @return A completable future to be completed with the response.
     */
    public CompletableFuture<Void> send(Object request) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        sendRequest((RaftRequest) request, (r, n) -> communicationService.unicast(r, r.type().subject(clusterName), SERIALIZER::encode, n), future);
        return future;
    }

    /**
     * Sends a request to the cluster and awaits a response.
     *
     * @param request The request to send.
     * @param <T>     The request type.
     * @param <U>     The response type.
     * @return A completable future to be completed with the response.
     */
    public <T, U> CompletableFuture<U> sendAndReceive(T request) {
        CompletableFuture<U> future = new CompletableFuture<>();
        sendRequest((RaftRequest) request, (r, n) -> communicationService.sendAndReceive(r, r.type().subject(clusterName), SERIALIZER::encode, SERIALIZER::decode, n), future);
        return future;
    }

    /**
     * Sends the given request attempt to the cluster.
     */
    protected <T extends RaftRequest, U> void sendRequest(T request, BiFunction<RaftRequest, NodeId, CompletableFuture<U>> sender, CompletableFuture<U> future) {
        if (open) {
            NodeId node = next();
            if (node != null) {
                logger().trace("{} - Sending {}", name(), request);
                sender.apply(request, node).whenComplete((r, e) -> {
                    if (e != null || r != null) {
                        handleResponse(request, sender, node, (RaftResponse) r, e, future);
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
            logger().trace("{} - Resetting connection. Reason: {}", name(), cause);
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
                logger().trace("{} - Received {}", name(), response);
                future.complete(response);
            } else {
                resendRequest(response.error().createException(), request, sender, node, future);
            }
        } else if (error instanceof ConnectException || error instanceof TimeoutException || error instanceof MessagingException || error instanceof ClosedChannelException) {
            resendRequest(error, request, sender, node, future);
        } else {
            logger().debug("{} - {} failed! Reason: {}", name(), request, error);
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

        // TODO: Does this need to be here?
        reset();

        if (!selector.hasNext()) {
            logger().debug("{} - Failed to connect to the cluster", name());
            return null;
        } else {
            this.node = selector.next();
            logger().debug("{} - Connecting to {}", name(), node);
            return node;
        }
    }

}
