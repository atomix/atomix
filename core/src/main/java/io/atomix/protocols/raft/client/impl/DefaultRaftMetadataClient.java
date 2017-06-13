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

import io.atomix.cluster.ClusterCommunicationService;
import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.client.CommunicationStrategies;
import io.atomix.protocols.raft.client.RaftMetadataClient;
import io.atomix.protocols.raft.session.impl.RaftClientConnection;
import io.atomix.protocols.raft.session.impl.NodeSelectorManager;
import io.atomix.protocols.raft.metadata.RaftSessionMetadata;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default Copycat metadata.
 */
public class DefaultRaftMetadataClient implements RaftMetadataClient {
  private final NodeSelectorManager selectorManager;
  private final RaftClientConnection connection;

  public DefaultRaftMetadataClient(String clientId, String clusterName, ClusterCommunicationService communicationService, NodeSelectorManager selectorManager) {
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.connection = new RaftClientConnection(clientId, clusterName, communicationService, selectorManager.createSelector(CommunicationStrategies.LEADER));
  }

  @Override
  public NodeId leader() {
    return selectorManager.leader();
  }

  @Override
  public Collection<NodeId> servers() {
    return selectorManager.servers();
  }

  /**
   * Requests metadata from the cluster.
   *
   * @return A completable future to be completed with cluster metadata.
   */
  private CompletableFuture<MetadataResponse> getMetadata() {
    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    connection.<MetadataRequest, MetadataResponse>sendAndReceive(MetadataRequest.builder().build()).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          future.complete(response);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Set<RaftSessionMetadata>> getSessions() {
    return getMetadata().thenApply(MetadataResponse::sessions);
  }

  @Override
  public CompletableFuture<Set<RaftSessionMetadata>> getSessions(String type) {
    return getMetadata().thenApply(response -> response.sessions().stream().filter(s -> s.type().equals(type)).collect(Collectors.toSet()));
  }

}
