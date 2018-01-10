/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.roles;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.protocol.RaftRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Abstract state.
 */
public abstract class AbstractRole implements RaftRole {
  protected final Logger log;
  protected final RaftContext raft;
  private boolean open = true;

  protected AbstractRole(RaftContext raft) {
    this.raft = raft;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftServer.class)
        .addValue(raft.getName())
        .add("role", role())
        .build());
  }

  /**
   * Returns the Raft state represented by this state.
   *
   * @return The Raft state represented by this state.
   */
  public abstract RaftServer.Role role();

  /**
   * Logs a request.
   */
  protected final <R extends RaftRequest> R logRequest(R request) {
    log.trace("Received {}", request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends RaftResponse> R logResponse(R response) {
    log.trace("Sending {}", response);
    return response;
  }

  @Override
  public CompletableFuture<RaftRole> start() {
    raft.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return open;
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends RaftRequest, U extends RaftResponse> CompletableFuture<U> forward(T request, BiFunction<NodeId, T, CompletableFuture<U>> function) {
    CompletableFuture<U> future = new CompletableFuture<>();
    DefaultRaftMember leader = raft.getLeader();
    if (leader == null) {
      return Futures.exceptionalFuture(new RaftException.NoLeader("No leader found"));
    }

    function.apply(leader.nodeId(), request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        future.complete(response);
      } else {
        future.completeExceptionally(error);
      }
    }, raft.getThreadContext());
    return future;
  }

  /**
   * Updates the term and leader.
   */
  protected boolean updateTermAndLeader(long term, NodeId leader) {
    // If the request indicates a term that is greater than the current term or no leader has been
    // set for the current term, update leader and term.
    if (term > raft.getTerm() || (term == raft.getTerm() && raft.getLeader() == null && leader != null)) {
      raft.setTerm(term);
      raft.setLeader(leader);

      // Reset the current cluster configuration to the last committed configuration when a leader change occurs.
      raft.getCluster().reset();
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<Void> stop() {
    raft.checkThread();
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("context", raft)
        .toString();
  }
}
