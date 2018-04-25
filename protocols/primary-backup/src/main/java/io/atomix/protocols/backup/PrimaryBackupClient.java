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
package io.atomix.protocols.backup;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.ProxyClient;
import io.atomix.primitive.proxy.impl.BlockingAwarePartitionProxy;
import io.atomix.primitive.proxy.impl.RecoveringPartitionProxy;
import io.atomix.primitive.proxy.impl.RetryingPartitionProxy;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.protocols.backup.protocol.PrimaryBackupClientProtocol;
import io.atomix.protocols.backup.protocol.PrimitiveDescriptor;
import io.atomix.protocols.backup.proxy.PrimaryBackupProxy;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup client.
 */
public class PrimaryBackupClient implements ProxyClient {

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
  private final PrimaryBackupClientProtocol protocol;
  private final PrimaryElection primaryElection;
  private final SessionIdService sessionIdService;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;

  public PrimaryBackupClient(
      String clientName,
      PartitionId partitionId,
      ClusterMembershipService clusterMembershipService,
      PrimaryBackupClientProtocol protocol,
      PrimaryElection primaryElection,
      SessionIdService sessionIdService,
      ThreadContextFactory threadContextFactory) {
    this.clientName = clientName;
    this.partitionId = partitionId;
    this.clusterMembershipService = clusterMembershipService;
    this.protocol = protocol;
    this.primaryElection = primaryElection;
    this.sessionIdService = sessionIdService;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
  }

  @Override
  public PrimaryBackupProxy.Builder proxyBuilder(String primitiveName, PrimitiveType primitiveType) {
    return new PrimaryBackupProxy.Builder() {
      @Override
      public PartitionProxy build() {
        Supplier<PartitionProxy> proxyBuilder = () -> new PrimaryBackupProxy(
            clientName,
            partitionId,
            sessionIdService.nextSessionId().join(),
            primitiveType,
            new PrimitiveDescriptor(
                primitiveName,
                primitiveType.id(),
                numBackups,
                replication),
            clusterMembershipService,
            PrimaryBackupClient.this.protocol,
            primaryElection,
            threadContextFactory.createContext());

        PartitionProxy proxy;
        if (recovery == Recovery.RECOVER) {
          proxy = new RecoveringPartitionProxy(
              clientName,
              partitionId,
              primitiveName,
              primitiveType,
              proxyBuilder,
              threadContextFactory.createContext());
        } else {
          proxy = proxyBuilder.get();
        }

        // If max retries is set, wrap the client in a retrying proxy client.
        if (maxRetries > 0) {
          proxy = new RetryingPartitionProxy(
              proxy,
              threadContextFactory.createContext(),
              maxRetries,
              retryDelay);
        }

        // Default the executor to use the configured thread pool executor and create a blocking aware proxy client.
        Executor executor = this.executor != null ? this.executor : threadContextFactory.createContext();
        return new BlockingAwarePartitionProxy(proxy, executor);
      }
    };
  }

  /**
   * Closes the primary-backup client.
   *
   * @return future to be completed once the client is closed
   */
  public CompletableFuture<Void> close() {
    // TODO: Close client proxies
    threadContext.close();
    threadContextFactory.close();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Primary-backup client builder.
   */
  public static class Builder implements io.atomix.utils.Builder<PrimaryBackupClient> {
    protected String clientName = "atomix";
    protected PartitionId partitionId;
    protected ClusterMembershipService clusterMembershipService;
    protected PrimaryBackupClientProtocol protocol;
    protected PrimaryElection primaryElection;
    protected SessionIdService sessionIdService;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Runtime.getRuntime().availableProcessors();
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
    public Builder withProtocol(PrimaryBackupClientProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol cannot be null");
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
     * Sets the session ID provider.
     *
     * @param sessionIdService the session ID provider
     * @return the client builder
     */
    public Builder withSessionIdProvider(SessionIdService sessionIdService) {
      this.sessionIdService = checkNotNull(sessionIdService, "sessionIdProvider cannot be null");
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
    public PrimaryBackupClient build() {
      Logger log = ContextualLoggerFactory.getLogger(PrimaryBackupClient.class, LoggerContext.builder(PrimaryBackupClient.class)
          .addValue(clientName)
          .build());
      ThreadContextFactory threadContextFactory = this.threadContextFactory != null
          ? this.threadContextFactory
          : threadModel.factory("backup-client-" + clientName + "-%d", threadPoolSize, log);
      return new PrimaryBackupClient(
          clientName,
          partitionId,
          clusterMembershipService,
          protocol,
          primaryElection,
          sessionIdService,
          threadContextFactory);
    }
  }
}
