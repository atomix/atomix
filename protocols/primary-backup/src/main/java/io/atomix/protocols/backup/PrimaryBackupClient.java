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

import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveException.Unavailable;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.BlockingAwarePrimitiveProxy;
import io.atomix.primitive.proxy.impl.RecoveringPrimitiveProxy;
import io.atomix.primitive.proxy.impl.RetryingPrimitiveProxy;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.PrimaryBackupClientProtocol;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.protocol.PrimitiveDescriptor;
import io.atomix.protocols.backup.proxy.PrimaryBackupProxy;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupSerializers;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup client.
 */
public class PrimaryBackupClient implements PrimitiveClient<MultiPrimaryProtocol> {

  /**
   * Returns a new primary-backup client builder.
   *
   * @return a new primary-backup client builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final Serializer SERIALIZER = PrimaryBackupSerializers.PROTOCOL;

  private final String clientName;
  private final ClusterService clusterService;
  private final PrimaryBackupClientProtocol protocol;
  private final PrimaryElection primaryElection;
  private final SessionIdService sessionIdService;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;

  public PrimaryBackupClient(
      String clientName,
      ClusterService clusterService,
      PrimaryBackupClientProtocol protocol,
      PrimaryElection primaryElection,
      SessionIdService sessionIdService,
      ThreadContextFactory threadContextFactory) {
    this.clientName = clientName;
    this.clusterService = clusterService;
    this.protocol = protocol;
    this.primaryElection = primaryElection;
    this.sessionIdService = sessionIdService;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveProxy newProxy(String primitiveName, PrimitiveType primitiveType, MultiPrimaryProtocol primitiveProtocol) {
    Supplier<PrimitiveProxy> proxyBuilder = () -> new PrimaryBackupProxy(
        clientName,
        sessionIdService.nextSessionId().join(),
        primitiveType,
        new PrimitiveDescriptor(
            primitiveName,
            primitiveType.id(),
            primitiveProtocol.backups(),
            primitiveProtocol.replication()),
        clusterService,
        PrimaryBackupClient.this.protocol,
        primaryElection,
        threadContextFactory.createContext());

    PrimitiveProxy proxy;
    if (primitiveProtocol.recovery() == Recovery.RECOVER) {
      proxy = new RecoveringPrimitiveProxy(
          clientName,
          primitiveName,
          primitiveType,
          proxyBuilder,
          threadContextFactory.createContext());
    } else {
      proxy = proxyBuilder.get();
    }

    // If max retries is set, wrap the client in a retrying proxy client.
    if (primitiveProtocol.maxRetries() > 0) {
      proxy = new RetryingPrimitiveProxy(
          proxy,
          threadContextFactory.createContext(),
          primitiveProtocol.maxRetries(),
          primitiveProtocol.retryDelay());
    }

    // Default the executor to use the configured thread pool executor and create a blocking aware proxy client.
    Executor executor = primitiveProtocol.executor() != null
        ? primitiveProtocol.executor()
        : threadContextFactory.createContext();
    return new BlockingAwarePrimitiveProxy(proxy, executor);
  }

  @Override
  public CompletableFuture<Set<String>> getPrimitives(PrimitiveType primitiveType) {
    CompletableFuture<Set<String>> future = new CompletableFuture<>();
    MetadataRequest request = MetadataRequest.request(primitiveType.id());
    threadContext.execute(() -> {
      NodeId primary = primaryElection.getTerm().join().primary();
      if (primary == null) {
        future.completeExceptionally(new Unavailable());
        return;
      }

      protocol.metadata(primary, request).whenCompleteAsync((response, error) -> {
        if (error == null) {
          if (response.status() == Status.OK) {
            future.complete(response.primitiveNames());
          } else {
            future.completeExceptionally(new PrimitiveException.Unavailable());
          }
        } else {
          future.completeExceptionally(new PrimitiveException.Unavailable());
        }
      }, threadContext);
    });
    return future;
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
    protected ClusterService clusterService;
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
     * Sets the cluster service.
     *
     * @param clusterService the cluster service
     * @return the client builder
     */
    public Builder withClusterService(ClusterService clusterService) {
      this.clusterService = checkNotNull(clusterService, "clusterService cannot be null");
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
          clusterService,
          protocol,
          primaryElection,
          sessionIdService,
          threadContextFactory);
    }
  }
}
