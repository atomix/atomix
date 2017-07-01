/*
 * Copyright 2015-present Open Networking Laboratory
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

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.RaftRequest;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Abstract state.
 */
public abstract class AbstractRole implements RaftRole {
  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final RaftServerContext context;
  private boolean open = true;

  protected AbstractRole(RaftServerContext context) {
    this.context = context;
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
    log.trace("{} Received {}", context.getName(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends RaftResponse> R logResponse(R response) {
    log.trace("{} Sending {}", context.getName(), response);
    return response;
  }

  @Override
  public CompletableFuture<RaftRole> open() {
    context.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends RaftRequest, U extends RaftResponse> CompletableFuture<U> forward(T request, BiFunction<MemberId, T, CompletableFuture<U>> function) {
    CompletableFuture<U> future = new CompletableFuture<>();
    DefaultRaftMember leader = context.getLeader();
    if (leader == null) {
      return Futures.exceptionalFuture(new RaftException.NoLeader("No leader found"));
    }

    function.apply(leader.memberId(), request).whenComplete((response, error) -> {
      if (error == null) {
        future.complete(response);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Updates the term and leader.
   */
  protected boolean updateTermAndLeader(long term, MemberId leader) {
    // If the request indicates a term that is greater than the current term or no leader has been
    // set for the current term, update leader and term.
    if (term > context.getTerm() || (term == context.getTerm() && context.getLeader() == null && leader != null)) {
      context.setTerm(term);
      context.setLeader(leader);

      // Reset the current cluster configuration to the last committed configuration when a leader change occurs.
      context.getClusterState().reset();
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<Void> close() {
    context.checkThread();
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("context", context)
        .toString();
  }

}
