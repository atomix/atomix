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
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.impl.PrimitiveServiceRegistry;
import io.atomix.protocols.backup.impl.DefaultPrimaryBackupServer;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadModel;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup server.
 */
public interface PrimaryBackupServer extends Managed<PrimaryBackupServer> {

  /**
   * Returns a new server builder.
   *
   * @return a new server builder
   */
  static Builder builder() {
    return new DefaultPrimaryBackupServer.Builder();
  }

  /**
   * Primary-backup server builder
   */
  abstract class Builder implements io.atomix.utils.Builder<PrimaryBackupServer> {
    protected String serverName = "atomix";
    protected ClusterService clusterService;
    protected ClusterCommunicationService communicationService;
    protected ReplicaInfoProvider replicaProvider;
    protected final PrimitiveServiceRegistry serviceRegistry = new PrimitiveServiceRegistry();
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Runtime.getRuntime().availableProcessors();

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
     * Sets the cluster communication service.
     *
     * @param communicationService the cluster communication service
     * @return the client builder
     */
    public Builder withCommunicationService(ClusterCommunicationService communicationService) {
      this.communicationService = checkNotNull(communicationService, "communicationService cannot be null");
      return this;
    }

    /**
     * Sets the replica provider.
     *
     * @param replicaProvider the replica provider
     * @return the client builder
     */
    public Builder withReplicaProvider(ReplicaInfoProvider replicaProvider) {
      this.replicaProvider = checkNotNull(replicaProvider, "replicaProvider cannot be null");
      return this;
    }

    /**
     * Adds a Raft service factory.
     *
     * @param type    The service type name.
     * @param factory The Raft service factory.
     * @return The server builder.
     * @throws NullPointerException if the {@code factory} is {@code null}
     */
    public Builder addService(PrimitiveType type, Supplier<PrimitiveService> factory) {
      return addService(type.id(), factory);
    }

    /**
     * Adds a Raft service factory.
     *
     * @param type    The service type name.
     * @param factory The Raft service factory.
     * @return The server builder.
     * @throws NullPointerException if the {@code factory} is {@code null}
     */
    public Builder addService(String type, Supplier<PrimitiveService> factory) {
      serviceRegistry.register(type, factory);
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
  }
}
