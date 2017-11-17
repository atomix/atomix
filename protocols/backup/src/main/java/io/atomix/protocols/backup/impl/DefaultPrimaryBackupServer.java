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
package io.atomix.protocols.backup.impl;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.impl.PrimitiveServiceRegistry;
import io.atomix.protocols.backup.PrimaryBackupServer;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.protocols.backup.protocol.CloseSessionRequest;
import io.atomix.protocols.backup.protocol.CloseSessionResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.OpenSessionRequest;
import io.atomix.protocols.backup.protocol.OpenSessionResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupNamespaces;
import io.atomix.serializer.Serializer;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Primary-backup server.
 */
public class DefaultPrimaryBackupServer implements PrimaryBackupServer {
  private static final Serializer SERIALIZER = Serializer.using(PrimaryBackupNamespaces.PROTOCOL);
  private final String serverName;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ThreadContextFactory threadContextFactory;
  private final PrimitiveServiceRegistry primitiveServices;
  private final ReplicaInfoProvider replicaProvider;
  private final Map<String, PrimaryBackupServiceContext> services = Maps.newConcurrentMap();
  private final MessageSubject openSessionSubject;
  private final MessageSubject closeSessionSubject;
  private final MessageSubject metadataSubject;
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultPrimaryBackupServer(
      String serverName,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ThreadContextFactory threadContextFactory,
      PrimitiveServiceRegistry primitiveServices,
      ReplicaInfoProvider replicaProvider) {
    this.serverName = serverName;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.threadContextFactory = threadContextFactory;
    this.primitiveServices = primitiveServices;
    this.replicaProvider = replicaProvider;
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
        PrimitiveType.from(request.primitiveType()),
        primitiveServices.getFactory(request.primitiveType()).get(),
        threadContextFactory.createContext(),
        clusterService,
        communicationService,
        replicaProvider))
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
   * Default primary-backup server builder.
   */
  public static class Builder extends PrimaryBackupServer.Builder {
    @Override
    public PrimaryBackupServer build() {
      Logger log = ContextualLoggerFactory.getLogger(DefaultPrimaryBackupServer.class, LoggerContext.builder(PrimaryBackupServer.class)
          .addValue(serverName)
          .build());
      ThreadContextFactory threadContextFactory = threadModel.factory("backup-server-" + serverName + "-%d", threadPoolSize, log);
      return new DefaultPrimaryBackupServer(
          serverName,
          clusterService,
          communicationService,
          threadContextFactory,
          serviceRegistry,
          replicaProvider);
    }
  }
}
