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

import io.atomix.cluster.NodeId;

import java.util.concurrent.CompletableFuture;

/**
 * Raft client protocol dispatcher.
 */
public interface RaftClientProtocolDispatcher {

    /**
     * Sends an open session request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<OpenSessionReply> openSession(NodeId nodeId, OpenSessionMessage request);

    /**
     * Sends a close session request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<CloseSessionReply> closeSession(NodeId nodeId, CloseSessionMessage request);

    /**
     * Sends a keep alive request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<KeepAliveReply> keepAlive(NodeId nodeId, KeepAliveMessage request);

    /**
     * Sends a query request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<QueryReply> query(NodeId nodeId, QueryMessage request);

    /**
     * Sends a command request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<CommandReply> command(NodeId nodeId, CommandMessage request);

    /**
     * Sends a metadata request to the given node.
     *
     * @param nodeId the node to which to send the request
     * @param request the request to send
     * @return a future to be completed with the response
     */
    CompletableFuture<MetadataReply> metadata(NodeId nodeId, MetadataMessage request);

    /**
     * Broadcasts a reset request to all nodes in the cluster.
     *
     * @param request the reset request to broadcast
     */
    void reset(ResetMessage request);

}
