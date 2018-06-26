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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.ManagedMemberGroupService;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupServerProtocol;
import io.atomix.protocols.backup.protocol.PrimitiveRequest;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Primary-backup server context.
 */
public class PrimaryBackupServerContext implements Managed<Void> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String serverName;
  private final ClusterMembershipService clusterMembershipService;
  private final ManagedMemberGroupService memberGroupService;
  private final PrimaryBackupServerProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private final boolean closeOnStop;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final PrimaryElection primaryElection;
  private final Map<String, CompletableFuture<PrimaryBackupServiceContext>> services = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public PrimaryBackupServerContext(
      String serverName,
      ClusterMembershipService clusterMembershipService,
      ManagedMemberGroupService memberGroupService,
      PrimaryBackupServerProtocol protocol,
      PrimitiveTypeRegistry primitiveTypes,
      PrimaryElection primaryElection,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.serverName = serverName;
    this.clusterMembershipService = clusterMembershipService;
    this.memberGroupService = memberGroupService;
    this.protocol = protocol;
    this.threadContextFactory = threadContextFactory;
    this.closeOnStop = closeOnStop;
    this.primitiveTypes = primitiveTypes;
    this.primaryElection = primaryElection;
  }

  /**
   * Returns the current server role.
   *
   * @return the current server role
   */
  public Role getRole() {
    return Objects.equals(Futures.get(primaryElection.getTerm()).primary().memberId(), clusterMembershipService.getLocalMember().id())
        ? Role.PRIMARY
        : Role.BACKUP;
  }

  @Override
  public CompletableFuture<Void> start() {
    registerListeners();
    return memberGroupService.start().thenCompose(v -> {
      MemberGroup group = memberGroupService.getMemberGroup(clusterMembershipService.getLocalMember());
      if (group != null) {
        return primaryElection.enter(new GroupMember(clusterMembershipService.getLocalMember().id(), group.id()));
      }
      return CompletableFuture.completedFuture(null);
    }).thenApply(v -> {
      started.set(true);
      return null;
    });
  }

  /**
   * Handles an execute request.
   */
  private CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    return getService(request).thenCompose(service -> service.execute(request));
  }

  /**
   * Handles a backup request.
   */
  private CompletableFuture<BackupResponse> backup(BackupRequest request) {
    return getService(request).thenCompose(service -> service.backup(request));
  }

  /**
   * Handles a restore request.
   */
  private CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    return getService(request).thenCompose(service -> service.restore(request));
  }

  /**
   * Handles a close request.
   */
  private CompletableFuture<CloseResponse> close(CloseRequest request) {
    return getService(request).thenCompose(service -> service.close(request));
  }

  /**
   * Returns the service context for the given request.
   */
  private CompletableFuture<PrimaryBackupServiceContext> getService(PrimitiveRequest request) {
    return services.computeIfAbsent(request.primitive().name(), n -> {
      PrimitiveType primitiveType = primitiveTypes.getPrimitiveType(request.primitive().type());
      PrimaryBackupServiceContext service = new PrimaryBackupServiceContext(
          serverName,
          PrimitiveId.from(request.primitive().name()),
          primitiveType,
          request.primitive(),
          threadContextFactory.createContext(),
          clusterMembershipService,
          memberGroupService,
          protocol,
          primaryElection);

      OrderedFuture<PrimaryBackupServiceContext> newOrderFuture = new OrderedFuture<>();
      service.open().whenComplete((v, e) -> {
        if (e != null) {
          newOrderFuture.completeExceptionally(e);
        } else {
          newOrderFuture.complete(service);
        }
      });
      return newOrderFuture;
    });
  }

  /**
   * Handles a metadata request.
   */
  private CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    return CompletableFuture.completedFuture(MetadataResponse.ok(services.entrySet().stream()
        .filter(entry -> Futures.get(entry.getValue()).serviceType().name().equals(request.primitiveType()))
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
    protocol.registerCloseHandler(this::close);
    protocol.registerMetadataHandler(this::metadata);
  }

  /**
   * Unregisters message listeners.
   */
  private void unregisterListeners() {
    protocol.unregisterExecuteHandler();
    protocol.unregisterBackupHandler();
    protocol.unregisterRestoreHandler();
    protocol.unregisterCloseHandler();
    protocol.unregisterMetadataHandler();
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    unregisterListeners();
    started.set(false);
    List<CompletableFuture<Void>> futures = services.values().stream()
        .map(future -> future.thenCompose(service -> service.close()))
        .collect(Collectors.toList());
    return Futures.allOf(futures).exceptionally(throwable -> {
      log.error("Failed closing services", throwable);
      return null;
    }).thenCompose(v -> memberGroupService.stop()).exceptionally(throwable -> {
      log.error("Failed stopping member group service", throwable);
      return null;
    }).thenRunAsync(() -> {
      if (closeOnStop) {
        threadContextFactory.close();
      }
    });
  }
}
