/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.impl;

import io.atomix.logging.LoggerFactory;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftMetadataClient;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.impl.NodeSelectorManager;
import io.atomix.protocols.raft.proxy.impl.RaftProxyManager;
import io.atomix.utils.concurrent.ThreadPoolContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default Raft client implementation.
 */
public class DefaultRaftClient implements RaftClient {
  private final String clientId;
  private final Collection<MemberId> cluster;
  private final ScheduledExecutorService threadPoolExecutor;
  private final RaftMetadataClient metadata;
  private final NodeSelectorManager selectorManager = new NodeSelectorManager();
  private final RaftProxyManager sessionManager;

  public DefaultRaftClient(
      String clientId,
      MemberId nodeId,
      Collection<MemberId> cluster,
      RaftClientProtocol protocol,
      ScheduledExecutorService threadPoolExecutor) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.cluster = checkNotNull(cluster, "cluster cannot be null");
    this.threadPoolExecutor = checkNotNull(threadPoolExecutor, "threadPoolExecutor cannot be null");
    this.metadata = new DefaultRaftMetadataClient(clientId, protocol, selectorManager, new ThreadPoolContext(threadPoolExecutor));
    this.sessionManager = new RaftProxyManager(clientId, nodeId, protocol, selectorManager, threadPoolExecutor);
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public RaftMetadataClient metadata() {
    return metadata;
  }

  @Override
  public synchronized CompletableFuture<RaftClient> connect(Collection<MemberId> cluster) {
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
  public RaftProxy.Builder newProxyBuilder() {
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
   * Default Raft session builder.
   */
  private class SessionBuilder extends RaftProxy.Builder {
    @Override
    public RaftProxy build() {
      return sessionManager.openSession(name, type, communicationStrategy, serializer, executor, timeout).join();
    }
  }

  /**
   * Default Raft client builder.
   */
  public static class Builder extends RaftClient.Builder {
    public Builder(Collection<MemberId> cluster) {
      super(cluster);
    }

    @Override
    public RaftClient build() {
      checkNotNull(nodeId, "nodeId cannot be null");
      ThreadFactory threadFactory = namedThreads("raft-client-" + clientId + "-%d", LoggerFactory.getLogger(RaftClient.class));
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
      return new DefaultRaftClient(clientId, nodeId, cluster, protocol, executor);
    }
  }
}
