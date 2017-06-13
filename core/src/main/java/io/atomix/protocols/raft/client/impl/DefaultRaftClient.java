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
package io.atomix.protocols.raft.client.impl;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.client.RaftClient;
import io.atomix.protocols.raft.client.RaftMetadataClient;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.impl.NodeSelectorManager;
import io.atomix.protocols.raft.session.impl.RaftSessionManager;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default Copycat client implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DefaultRaftClient implements RaftClient {
    private final String clientId;
    private final Collection<NodeId> cluster;
    private final ScheduledExecutorService threadPoolExecutor;
    private final RaftMetadataClient metadata;
    private final NodeSelectorManager selectorManager = new NodeSelectorManager();
    private final RaftSessionManager sessionManager;

    public DefaultRaftClient(
            String clientId,
            Collection<NodeId> cluster,
            RaftClientProtocol protocol,
            ScheduledExecutorService threadPoolExecutor) {
        this.clientId = checkNotNull(clientId, "clientId cannot be null");
        this.cluster = checkNotNull(cluster, "cluster cannot be null");
        this.threadPoolExecutor = checkNotNull(threadPoolExecutor, "threadPoolExecutor cannot be null");
        this.metadata = new DefaultRaftMetadataClient(clientId, protocol, selectorManager);
        this.sessionManager = new RaftSessionManager(clientId, protocol, selectorManager, threadPoolExecutor);
    }

    @Override
    public String id() {
        return clientId;
    }

    @Override
    public RaftMetadataClient metadata() {
        return metadata;
    }

    @Override
    public synchronized CompletableFuture<RaftClient> connect(Collection<NodeId> cluster) {
        CompletableFuture<RaftClient> future = new CompletableFuture<>();

        // If the provided cluster list is null or empty, use the default list.
        if (cluster == null || cluster.isEmpty()) {
            cluster = this.cluster;
        }

        // If the default list is null or empty, use the default host:port.
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("No cluster specified");
        }

        // Reset the connection list to allow the selection strategy to prioritize connections.
        sessionManager.resetConnections(null, cluster);

        // Register the session manager.
        sessionManager.open().whenCompleteAsync((result, error) -> {
            if (error == null) {
                future.complete(this);
            } else {
                future.completeExceptionally(error);
            }
        }, threadPoolExecutor);
        return future;
    }

    @Override
    public RaftSession.Builder sessionBuilder() {
        return new SessionBuilder();
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        return sessionManager.close();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", clientId)
                .toString();
    }

    /**
     * Default Copycat session builder.
     */
    private class SessionBuilder extends RaftSession.Builder {
        @Override
        public RaftSession build() {
            return sessionManager.openSession(name, type, communicationStrategy, serializer, timeout).join();
        }
    }
}
