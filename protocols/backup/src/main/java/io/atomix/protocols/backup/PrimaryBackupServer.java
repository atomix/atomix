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

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.backup.impl.PrimaryBackupServiceContext;
import io.atomix.protocols.backup.protocol.CloseSessionRequest;
import io.atomix.protocols.backup.protocol.CloseSessionResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.OpenSessionRequest;
import io.atomix.protocols.backup.protocol.OpenSessionResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupNamespaces;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

  private static final Serializer SERIALIZER = Serializer.using(PrimaryBackupNamespaces.PROTOCOL);
  private final String serverName;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ThreadContextFactory threadContextFactory;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimaryElection primaryElection;
  private final Map<String, PrimaryBackupServiceContext> services = Maps.newConcurrentMap();
  private final MessageSubject openSessionSubject;
  private final MessageSubject closeSessionSubject;
  private final MessageSubject metadataSubject;
  private final AtomicBoolean open = new AtomicBoolean();

  public PrimaryBackupServer(
      String serverName,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ThreadContextFactory threadContextFactory,
      PrimitiveTypeRegistry primitiveTypes,
      PrimaryElection primaryElection) {
    this.serverName = serverName;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.threadContextFactory = threadContextFactory;
    this.primitiveTypes = primitiveTypes;
    this.primaryElection = primaryElection;
    this.openSessionSubject = new MessageSubject(String.format("%s-open", serverName));
    this.closeSessionSubject = new MessageSubject(String.format("%s-close", serverName));
    this.metadataSubject = new MessageSubject(String.format("%s-metadata", serverName));
  }

  /**
   * Handles an open session request.
   */
  private CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    return services.computeIfAbsent(request.primitiveName(), n -> new PrimaryBackupServiceContext(
        serverName,
        PrimitiveId.from(request.primitiveName()),
        request.primitiveName(),
        primitiveTypes.get(request.primitiveType()),
        threadContextFactory.createContext(),
        clusterService,
        communicationService,
        primaryElection))
        .openSession(request);
  }

  /**
   * Handles a close session request.
   */
  private CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    PrimaryBackupServiceContext service = services.get(request.primitiveName());
    if (service != null) {
      return service.closeSession(request);
    }
    return CompletableFuture.completedFuture(new CloseSessionResponse(Status.ERROR));
  }

  /**
   * Handles a metadata request.
   */
  private CompletableFuture<MetadataResponse> getMetadata(MetadataRequest request) {
    return CompletableFuture.completedFuture(new MetadataResponse(Status.OK, services.entrySet().stream()
        .filter(entry -> entry.getValue().serviceType().id().equals(request.primitiveType()))
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet())));
  }

  /**
   * Registers message subscribers.
   */
  private void registerSubscribers() {
    communicationService.<OpenSessionRequest, OpenSessionResponse>addSubscriber(
        openSessionSubject,
        SERIALIZER::decode,
        this::openSession,
        SERIALIZER::encode);
    communicationService.<CloseSessionRequest, CloseSessionResponse>addSubscriber(
        closeSessionSubject,
        SERIALIZER::decode,
        this::closeSession,
        SERIALIZER::encode);
    communicationService.<MetadataRequest, MetadataResponse>addSubscriber(
        metadataSubject,
        SERIALIZER::decode,
        this::getMetadata,
        SERIALIZER::encode);
  }

  /**
   * Unregisters message subscribers.
   */
  private void unregisterSubscribers() {
    communicationService.removeSubscriber(openSessionSubject);
    communicationService.removeSubscriber(closeSessionSubject);
    communicationService.removeSubscriber(metadataSubject);
  }

  @Override
  public CompletableFuture<PrimaryBackupServer> open() {
    primaryElection.enter(clusterService.getLocalNode().id());
    registerSubscribers();
    open.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    unregisterSubscribers();
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }

  /**
   * Primary-backup server builder
   */
  public static class Builder implements io.atomix.utils.Builder<PrimaryBackupServer> {
    protected String serverName = "atomix";
    protected ClusterService clusterService;
    protected ClusterCommunicationService communicationService;
    protected PrimaryElection primaryElection;
    protected PrimitiveTypeRegistry primitiveTypes = new PrimitiveTypeRegistry();
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Runtime.getRuntime().availableProcessors();
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
     * Adds a primitive type to the registry.
     *
     * @param primitiveType the primitive type to add
     * @return the server builder
     * @throws NullPointerException if the {@code primitiveType} is {@code null}
     */
    public Builder addPrimitiveType(PrimitiveType primitiveType) {
      primitiveTypes.register(primitiveType);
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
      ThreadContextFactory threadContextFactory = this.threadContextFactory != null
          ? this.threadContextFactory
          : threadModel.factory("backup-server-" + serverName + "-%d", threadPoolSize, log);
      return new PrimaryBackupServer(
          serverName,
          clusterService,
          communicationService,
          threadContextFactory,
          primitiveTypes,
          primaryElection);
    }
  }
}
