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
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupServerProtocol;
import io.atomix.protocols.backup.protocol.PrimitiveRequest;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.concurrent.ThreadContextFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Primary-backup server context.
 */
public class PrimaryBackupServerContext {
  private final String serverName;
  private final ClusterService clusterService;
  private final PrimaryBackupServerProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimaryElection primaryElection;
  private final Map<String, PrimaryBackupServiceContext> services = Maps.newConcurrentMap();

  public PrimaryBackupServerContext(
      String serverName,
      ClusterService clusterService,
      PrimaryBackupServerProtocol protocol,
      ThreadContextFactory threadContextFactory,
      PrimitiveTypeRegistry primitiveTypes,
      PrimaryElection primaryElection) {
    this.serverName = serverName;
    this.clusterService = clusterService;
    this.protocol = protocol;
    this.threadContextFactory = threadContextFactory;
    this.primitiveTypes = primitiveTypes;
    this.primaryElection = primaryElection;
  }

  /**
   * Opens the server context.
   */
  public void open() {
    registerListeners();
  }

  /**
   * Handles an execute request.
   */
  private CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    return getService(request).execute(request);
  }

  /**
   * Handles a backup request.
   */
  private CompletableFuture<BackupResponse> backup(BackupRequest request) {
    return getService(request).backup(request);
  }

  /**
   * Handles a restore request.
   */
  private CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    return getService(request).restore(request);
  }

  /**
   * Returns the service context for the given request.
   */
  private PrimaryBackupServiceContext getService(PrimitiveRequest request) {
    return services.computeIfAbsent(request.primitive().name(), n -> new PrimaryBackupServiceContext(
        serverName,
        PrimitiveId.from(request.primitive().name()),
        primitiveTypes.get(request.primitive().type()),
        request.primitive(),
        threadContextFactory.createContext(),
        clusterService,
        protocol,
        primaryElection));
  }

  /**
   * Handles a metadata request.
   */
  private CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    return CompletableFuture.completedFuture(MetadataResponse.ok(services.entrySet().stream()
        .filter(entry -> entry.getValue().serviceType().id().equals(request.primitiveType()))
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet())));
  }

  /**
   * Registers message listeners.
   */
  private void registerListeners() {
    protocol.registerExecuteHandler(this::execute);
    protocol.registerBackupHandler(this::backup);
    protocol.registerRestoreHandler(this::restore);
    protocol.registerMetadataHandler(this::metadata);
  }

  /**
   * Unregisters message listeners.
   */
  private void unregisterListeners() {
    protocol.unregisterExecuteHandler();
    protocol.unregisterBackupHandler();
    protocol.unregisterRestoreHandler();
    protocol.unregisterMetadataHandler();
  }

  /**
   * Closes the server context.
   */
  public void close() {
    unregisterListeners();
  }
}
