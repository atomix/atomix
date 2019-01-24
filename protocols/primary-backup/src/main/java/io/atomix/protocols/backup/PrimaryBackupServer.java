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
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.impl.ClasspathScanningPrimitiveTypeRegistry;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.impl.DefaultMemberGroupService;
import io.atomix.protocols.backup.impl.PrimaryBackupServerContext;
import io.atomix.protocols.backup.protocol.PrimaryBackupServerProtocol;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup server.
 */
public class PrimaryBackupServer implements Managed<PrimaryBackupServer> {

  /**
   * Returns a new server builder.
   *
   * @return a new server builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Primary-backup role.
   */
  public enum Role {

    /**
     * Primary service role.
     */
    PRIMARY,

    /**
     * Backup service role.
     */
    BACKUP,

    /**
     * None service role.
     */
    NONE,
  }

  private final PrimaryBackupServerContext context;

  private PrimaryBackupServer(PrimaryBackupServerContext context) {
    this.context = checkNotNull(context, "context cannot be null");
  }

  /**
   * Returns the current server role.
   *
   * @return the current server role
   */
  public Role getRole() {
    return context.getRole();
  }

  @Override
  public CompletableFuture<PrimaryBackupServer> start() {
    return context.start().thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return context.isRunning();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return context.stop();
  }

  /**
   * Primary-backup server builder
   */
  public static class Builder implements io.atomix.utils.Builder<PrimaryBackupServer> {
    protected String serverName = "atomix";
    protected ClusterMembershipService membershipService;
    protected PrimaryBackupServerProtocol protocol;
    protected PrimaryElection primaryElection;
    protected PrimitiveTypeRegistry primitiveTypes;
    protected MemberGroupProvider memberGroupProvider;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    protected ThreadContextFactory threadContextFactory;

    /**
     * Sets the server name.
     *
     * @param serverName The server name.
     * @return The server builder.
     * @throws NullPointerException if {@code serverName} is null
     */
    public Builder withServerName(String serverName) {
      this.serverName = checkNotNull(serverName, "server cannot be null");
      return this;
    }

    /**
     * Sets the cluster membership service.
     *
     * @param membershipService the cluster membership service
     * @return the server builder
     */
    public Builder withMembershipService(ClusterMembershipService membershipService) {
      this.membershipService = checkNotNull(membershipService, "membershipService cannot be null");
      return this;
    }

    /**
     * Sets the protocol.
     *
     * @param protocol the protocol
     * @return the server builder
     */
    public Builder withProtocol(PrimaryBackupServerProtocol protocol) {
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
     * Sets the primitive types.
     *
     * @param primitiveTypes the primitive types
     * @return the server builder
     * @throws NullPointerException if the {@code primitiveTypes} argument is {@code null}
     */
    public Builder withPrimitiveTypes(PrimitiveTypeRegistry primitiveTypes) {
      this.primitiveTypes = checkNotNull(primitiveTypes, "primitiveTypes cannot be null");
      return this;
    }

    /**
     * Sets the member group provider.
     *
     * @param memberGroupProvider the member group provider
     * @return the partition group builder
     */
    public Builder withMemberGroupProvider(MemberGroupProvider memberGroupProvider) {
      this.memberGroupProvider = checkNotNull(memberGroupProvider);
      return this;
    }

    /**
     * Sets the client thread model.
     *
     * @param threadModel the client thread model
     * @return the server builder
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
     * @return The server builder.
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
     * @return the server builder
     * @throws NullPointerException if the factory is null
     */
    public Builder withThreadContextFactory(ThreadContextFactory threadContextFactory) {
      this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
      return this;
    }

    @Override
    public PrimaryBackupServer build() {
      Logger log = ContextualLoggerFactory.getLogger(PrimaryBackupServer.class, LoggerContext.builder(PrimaryBackupServer.class)
          .addValue(serverName)
          .build());

      // If a ThreadContextFactory was not provided, create one and ensure it's closed when the server is stopped.
      boolean closeOnStop;
      ThreadContextFactory threadContextFactory;
      if (this.threadContextFactory == null) {
        threadContextFactory = threadModel.factory("backup-server-" + serverName + "-%d", threadPoolSize, log);
        closeOnStop = true;
      } else {
        threadContextFactory = this.threadContextFactory;
        closeOnStop = false;
      }

      return new PrimaryBackupServer(new PrimaryBackupServerContext(
          serverName,
          membershipService,
          new DefaultMemberGroupService(membershipService, memberGroupProvider),
          protocol,
          primitiveTypes != null ? primitiveTypes
              : new ClasspathScanningPrimitiveTypeRegistry(Thread.currentThread().getContextClassLoader()),
          primaryElection,
          threadContextFactory,
          closeOnStop));
    }
  }
}
