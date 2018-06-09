/*
 * Copyright 2017-present Open Networking Foundation
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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.session.SessionMetadata;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftMetadataClient;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.protocols.raft.session.impl.MemberSelectorManager;
import io.atomix.protocols.raft.session.impl.RaftSessionConnection;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.LoggerContext;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default Raft metadata.
 */
public class DefaultRaftMetadataClient implements RaftMetadataClient {
  private final MemberSelectorManager selectorManager;
  private final RaftSessionConnection connection;

  public DefaultRaftMetadataClient(String clientId, RaftClientProtocol protocol, MemberSelectorManager selectorManager, ThreadContext context) {
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.connection = new RaftSessionConnection(
        protocol,
        selectorManager.createSelector(CommunicationStrategy.LEADER),
        context,
        LoggerContext.builder(RaftClient.class)
            .addValue(clientId)
            .build());
  }

  @Override
  public MemberId getLeader() {
    return selectorManager.leader();
  }

  @Override
  public Collection<MemberId> getMembers() {
    return selectorManager.members();
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
  public CompletableFuture<Set<SessionMetadata>> getSessions() {
    return getMetadata().thenApply(MetadataResponse::sessions);
  }

  @Override
  public CompletableFuture<Set<SessionMetadata>> getSessions(PrimitiveType primitiveType) {
    return getMetadata().thenApply(response -> response.sessions()
        .stream()
        .filter(s -> s.primitiveType().equals(primitiveType.name()))
        .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Set<SessionMetadata>> getSessions(PrimitiveType primitiveType, String serviceName) {
    return getMetadata().thenApply(response -> response.sessions()
        .stream()
        .filter(s -> s.primitiveType().equals(primitiveType.name()) && s.primitiveName().equals(serviceName))
        .collect(Collectors.toSet()));
  }
}
