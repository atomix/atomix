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

import io.atomix.protocols.raft.CommunicationStrategies;
import io.atomix.protocols.raft.RaftMetadataClient;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.metadata.RaftSessionMetadata;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.proxy.impl.NodeSelectorManager;
import io.atomix.protocols.raft.proxy.impl.RaftConnection;

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
  private final RaftConnection connection;

  public DefaultRaftMetadataClient(String clientId, RaftClientProtocol protocol, NodeSelectorManager selectorManager) {
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.connection = new RaftConnection(clientId, protocol.dispatcher(), selectorManager.createSelector(CommunicationStrategies.LEADER));
  }

  @Override
  public MemberId leader() {
    return selectorManager.leader();
  }

  @Override
  public Collection<MemberId> servers() {
    return selectorManager.servers();
  }

  /**
   * Requests metadata from the cluster.
   *
   * @return A completable future to be completed with cluster metadata.
   */
  private CompletableFuture<MetadataResponse> getMetadata() {
    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    connection.metadata(MetadataRequest.builder().build()).whenComplete((response, error) -> {
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
