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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Test server protocol.
 */
public class TestPrimaryBackupServerProtocol extends TestPrimaryBackupProtocol implements PrimaryBackupServerProtocol {
  private Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> executeHandler;
  private Function<CloseRequest, CompletableFuture<CloseResponse>> closeHandler;
  private Function<BackupRequest, CompletableFuture<BackupResponse>> backupHandler;
  private Function<RestoreRequest, CompletableFuture<RestoreResponse>> restoreHandler;
  private Function<MetadataRequest, CompletableFuture<MetadataResponse>> metadataHandler;

  public TestPrimaryBackupServerProtocol(MemberId memberId, Map<MemberId, TestPrimaryBackupServerProtocol> servers, Map<MemberId, TestPrimaryBackupClientProtocol> clients) {
    super(servers, clients);
    servers.put(memberId, this);
  }

  private CompletableFuture<TestPrimaryBackupServerProtocol> getServer(MemberId memberId) {
    TestPrimaryBackupServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  private CompletableFuture<TestPrimaryBackupClientProtocol> getClient(MemberId memberId) {
    TestPrimaryBackupClientProtocol client = client(memberId);
    if (client != null) {
      return Futures.completedFuture(client);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request) {
    return getServer(memberId).thenCompose(server -> server.backup(request));
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(MemberId memberId, RestoreRequest request) {
    return getServer(memberId).thenCompose(server -> server.restore(request));
  }

  @Override
  public void event(MemberId memberId, SessionId session, PrimitiveEvent event) {
    getClient(memberId).thenAccept(client -> client.event(session, event));
  }

  CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    if (executeHandler != null) {
      return executeHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<CloseResponse> close(CloseRequest request) {
    if (closeHandler != null) {
      return closeHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<BackupResponse> backup(BackupRequest request) {
    if (backupHandler != null) {
      return backupHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    if (restoreHandler != null) {
      return restoreHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    if (metadataHandler != null) {
      return metadataHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler) {
    this.executeHandler = handler;
  }

  @Override
  public void unregisterExecuteHandler() {
    this.executeHandler = null;
  }

  @Override
  public void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler) {
    this.backupHandler = handler;
  }

  @Override
  public void unregisterBackupHandler() {
    this.backupHandler = null;
  }

  @Override
  public void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler) {
    this.restoreHandler = handler;
  }

  @Override
  public void unregisterRestoreHandler() {
    this.restoreHandler = null;
  }

  @Override
  public void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler) {
    this.closeHandler = handler;
  }

  @Override
  public void unregisterCloseHandler() {
    this.closeHandler = null;
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    this.metadataHandler = handler;
  }

  @Override
  public void unregisterMetadataHandler() {
    this.metadataHandler = null;
  }
}
