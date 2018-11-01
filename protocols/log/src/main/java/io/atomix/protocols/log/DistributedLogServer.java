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

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.MemberGroupProvider;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.impl.DefaultMemberGroupService;
import io.atomix.protocols.log.impl.DistributedLogServerContext;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log server.
 */
public class DistributedLogServer implements Managed<DistributedLogServer> {

  /**
   * Returns a new server builder.
   *
   * @return a new server builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Log server role.
   */
  public enum Role {

    /**
     * Leader role.
     */
    LEADER,

    /**
     * Follower role.
     */
    FOLLOWER,

    /**
     * None role.
     */
    NONE,
  }

  final DistributedLogServerContext context;

  private DistributedLogServer(DistributedLogServerContext context) {
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
  public CompletableFuture<DistributedLogServer> start() {
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
   * Log server builder
   */
  public static class Builder implements io.atomix.utils.Builder<DistributedLogServer> {
    private static final String DEFAULT_SERVER_NAME = "atomix";
    private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
    private static final int DEFAULT_REPLICATION_FACTOR = 2;
    private static final Replication DEFAULT_REPLICATION_STRATEGY = Replication.SYNCHRONOUS;
    private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
    private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024;
    private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;
    private static final long DEFAULT_MAX_LOG_SIZE = 1024 * 1024 * 1024;
    private static final Duration DEFAULT_MAX_LOG_AGE = null;
    private static final double DEFAULT_INDEX_DENSITY = .005;

    protected String serverName = DEFAULT_SERVER_NAME;
    protected ClusterMembershipService membershipService;
    protected LogServerProtocol protocol;
    protected PrimaryElection primaryElection;
    protected MemberGroupProvider memberGroupProvider;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    protected ThreadContextFactory threadContextFactory;
    protected int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    protected Replication replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
    protected StorageLevel storageLevel = StorageLevel.DISK;
    protected File directory = new File(DEFAULT_DIRECTORY);
    protected int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
    protected int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
    protected double indexDensity = DEFAULT_INDEX_DENSITY;
    private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;
    protected long maxLogSize = DEFAULT_MAX_LOG_SIZE;
    protected Duration maxLogAge = DEFAULT_MAX_LOG_AGE;

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
    public Builder withProtocol(LogServerProtocol protocol) {
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

    /**
     * Sets the server replication factor.
     *
     * @param replicationFactor the server replication factor
     * @return the server builder
     * @throws IllegalArgumentException if the replication factor is not positive
     */
    public Builder withReplicationFactor(int replicationFactor) {
      checkArgument(replicationFactor > 0, "replicationFactor must be positive");
      this.replicationFactor = replicationFactor;
      return this;
    }

    /**
     * Sets the server replication strategy.
     *
     * @param replicationStrategy the server replication strategy
     * @return the server builder
     * @throws NullPointerException if the replication strategy is null
     */
    public Builder withReplicationStrategy(Replication replicationStrategy) {
      this.replicationStrategy = checkNotNull(replicationStrategy, "replicationStrategy cannot be null");
      return this;
    }

    /**
     * Sets the log storage level, returning the builder for method chaining.
     * <p>
     * The storage level indicates how individual entries should be persisted in the journal.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      return withDirectory(new File(checkNotNull(directory, "directory cannot be null")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      this.directory = checkNotNull(directory, "directory cannot be null");
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     * <p>
     * The maximum segment size dictates when logs should roll over to new segments. As entries are written to a segment
     * of the log, once the size of the segment surpasses the configured maximum segment size, the log will create a new
     * segment and append new entries to that segment.
     * <p>
     * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      this.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum entry size in bytes, returning the builder for method chaining.
     *
     * @param maxEntrySize the maximum entry size in bytes
     * @return the storage builder
     * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      checkArgument(maxEntrySize > 0, "maxEntrySize must be positive");
      this.maxEntrySize = maxEntrySize;
      return this;
    }

    /**
     * Sets the journal index density.
     * <p>
     * The index density is the frequency at which the position of entries written to the journal will be recorded in an
     * in-memory index for faster seeking.
     *
     * @param indexDensity the index density
     * @return the journal builder
     * @throws IllegalArgumentException if the density is not between 0 and 1
     */
    public Builder withIndexDensity(double indexDensity) {
      checkArgument(indexDensity > 0 && indexDensity < 1, "index density must be between 0 and 1");
      this.indexDensity = indexDensity;
      return this;
    }

    /**
     * Enables flushing buffers to disk when entries are committed to a segment, returning the builder for method
     * chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
     * committed in a given segment.
     *
     * @return The storage builder.
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder for method
     * chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
     * committed in a given segment.
     *
     * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
     * @return The storage builder.
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      this.flushOnCommit = flushOnCommit;
      return this;
    }

    /**
     * Sets the maximum log size.
     *
     * @param maxLogSize the maximum log size
     * @return the log server builder
     */
    public Builder withMaxLogSize(long maxLogSize) {
      this.maxLogSize = maxLogSize;
      return this;
    }

    /**
     * Sets the maximum log age.
     *
     * @param maxLogAge the maximum log age
     * @return the log server builder
     */
    public Builder withMaxLogAge(Duration maxLogAge) {
      this.maxLogAge = maxLogAge;
      return this;
    }

    @Override
    public DistributedLogServer build() {
      Logger log = ContextualLoggerFactory.getLogger(DistributedLogServer.class, LoggerContext.builder(DistributedLogServer.class)
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

      SegmentedJournal<LogEntry> journal = SegmentedJournal.<LogEntry>builder()
          .withName(serverName)
          .withDirectory(directory)
          .withStorageLevel(storageLevel)
          .withNamespace(Namespace.builder()
              .register(Namespaces.BASIC)
              .register(LogEntry.class)
              .register(byte[].class)
              .build())
          .withMaxSegmentSize(maxSegmentSize)
          .withMaxEntrySize(maxEntrySize)
          .withIndexDensity(indexDensity)
          .withFlushOnCommit(flushOnCommit)
          .build();

      return new DistributedLogServer(new DistributedLogServerContext(
          serverName,
          membershipService,
          new DefaultMemberGroupService(membershipService, memberGroupProvider),
          protocol,
          primaryElection,
          replicationFactor,
          replicationStrategy,
          journal,
          maxLogSize,
          maxLogAge,
          threadContextFactory,
          closeOnStop));
    }
  }
}
