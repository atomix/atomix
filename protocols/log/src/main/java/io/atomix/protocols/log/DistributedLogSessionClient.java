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
package io.atomix.protocols.log;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.log.impl.DistributedLogSession;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed log client.
 */
public class DistributedLogSessionClient {

  /**
   * Returns a new primary-backup client builder.
   *
   * @return a new primary-backup client builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private final String clientName;
  private final PartitionId partitionId;
  private final ClusterMembershipService clusterMembershipService;
  private final LogClientProtocol protocol;
  private final Supplier<CompletableFuture<SessionId>> sessionIdProvider;
  private final PrimaryElection primaryElection;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final boolean closeOnStop;

  public DistributedLogSessionClient(
      String clientName,
      PartitionId partitionId,
      ClusterMembershipService clusterMembershipService,
      LogClientProtocol protocol,
      Supplier<CompletableFuture<SessionId>> sessionIdProvider,
      PrimaryElection primaryElection,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.clientName = clientName;
    this.partitionId = partitionId;
    this.clusterMembershipService = clusterMembershipService;
    this.protocol = protocol;
    this.sessionIdProvider = sessionIdProvider;
    this.primaryElection = primaryElection;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
    this.closeOnStop = closeOnStop;
  }

  /**
   * Returns a new distributed log session builder.
   *
   * @return a new distributed log session builder
   */
  public LogSession.Builder sessionBuilder() {
    return new DistributedLogSession.Builder() {
      @Override
      public LogSession build() {
        return new DistributedLogSession(
            partitionId,
            sessionIdProvider.get().join(),
            clusterMembershipService,
            protocol,
            primaryElection,
            threadContextFactory.createContext());
      }
    };
  }

  /**
   * Closes the primary-backup client.
   *
   * @return future to be completed once the client is closed
   */
  public CompletableFuture<Void> close() {
    threadContext.close();
    if (closeOnStop) {
      threadContextFactory.close();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Primary-backup client builder.
   */
  public static class Builder implements io.atomix.utils.Builder<DistributedLogSessionClient> {
    protected String clientName = "atomix";
    protected PartitionId partitionId;
    protected ClusterMembershipService clusterMembershipService;
    protected LogClientProtocol protocol;
    protected Supplier<CompletableFuture<SessionId>> sessionIdProvider;
    protected PrimaryElection primaryElection;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    protected ThreadContextFactory threadContextFactory;

    /**
     * Sets the client name.
     *
     * @param clientName The client name.
     * @return The client builder.
     * @throws NullPointerException if {@code clientName} is null
     */
    public Builder withClientName(String clientName) {
      this.clientName = checkNotNull(clientName, "clientName cannot be null");
      return this;
    }

    /**
     * Sets the client partition ID.
     *
     * @param partitionId the client partition ID
     * @return the client builder
     */
    public Builder withPartitionId(PartitionId partitionId) {
      this.partitionId = checkNotNull(partitionId, "partitionId cannot be null");
      return this;
    }

    /**
     * Sets the cluster membership service.
     *
     * @param membershipService the cluster membership service
     * @return the client builder
     */
    public Builder withMembershipService(ClusterMembershipService membershipService) {
      this.clusterMembershipService = checkNotNull(membershipService, "membershipService cannot be null");
      return this;
    }

    /**
     * Sets the client protocol.
     *
     * @param protocol the client protocol
     * @return the client builder
     */
    public Builder withProtocol(LogClientProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol cannot be null");
      return this;
    }

    /**
     * Sets the session ID service
     *
     * @param sessionIdProvider the session ID service
     * @return the client builder
     */
    public Builder withSessionIdProvider(Supplier<CompletableFuture<SessionId>> sessionIdProvider) {
      this.sessionIdProvider = checkNotNull(sessionIdProvider, "sessionIdProvider cannot be null");
      return this;
    }

    /**
     * Sets the primary election.
     *
     * @param primaryElection the primary election
     * @return the client builder
     */
    public Builder withPrimaryElection(PrimaryElection primaryElection) {
      this.primaryElection = checkNotNull(primaryElection, "primaryElection cannot be null");
      return this;
    }

    /**
     * Sets the client thread model.
     *
     * @param threadModel the client thread model
     * @return the client builder
     * @throws NullPointerException if the thread model is null
     */
    public Builder withThreadModel(ThreadModel threadModel) {
      this.threadModel = checkNotNull(threadModel, "threadModel cannot be null");
      return this;
    }

    /**
     * Sets the client thread pool size.
     *
     * @param threadPoolSize The client thread pool size.
     * @return The client builder.
     * @throws IllegalArgumentException if the thread pool size is not positive
     */
    public Builder withThreadPoolSize(int threadPoolSize) {
      checkArgument(threadPoolSize > 0, "threadPoolSize must be positive");
      this.threadPoolSize = threadPoolSize;
      return this;
    }

    /**
     * Sets the client thread context factory.
     *
     * @param threadContextFactory the client thread context factory
     * @return the client builder
     * @throws NullPointerException if the factory is null
     */
    public Builder withThreadContextFactory(ThreadContextFactory threadContextFactory) {
      this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
      return this;
    }

    @Override
    public DistributedLogSessionClient build() {
      Logger log = ContextualLoggerFactory.getLogger(DistributedLogSessionClient.class, LoggerContext.builder(DistributedLogSessionClient.class)
          .addValue(clientName)
          .build());

      // If a ThreadContextFactory was not provided, create one and ensure it's closed when the client is stopped.
      boolean closeOnStop;
      ThreadContextFactory threadContextFactory;
      if (this.threadContextFactory == null) {
        threadContextFactory = threadModel.factory("log-client-" + clientName + "-%d", threadPoolSize, log);
        closeOnStop = true;
      } else {
        threadContextFactory = this.threadContextFactory;
        closeOnStop = false;
      }

      return new DistributedLogSessionClient(
          clientName,
          partitionId,
          clusterMembershipService,
          protocol,
          sessionIdProvider,
          primaryElection,
          threadContextFactory,
          closeOnStop);
    }
  }
}
