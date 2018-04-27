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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Primary-backup server protocol.
 */
public interface PrimaryBackupServerProtocol {

  /**
   * Sends a backup request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request);

  /**
   * Sends a restore request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<RestoreResponse> restore(MemberId memberId, RestoreRequest request);

  /**
   * Sends a primitive event to the given node.
   *
   * @param memberId  the node to which to publish the event
   * @param session the session to which to publish the event
   * @param event   the primitive event to publish
   */
  void event(MemberId memberId, SessionId session, PrimitiveEvent event);

  /**
   * Registers a execute request callback.
   *
   * @param handler the execute request handler to register
   */
  void registerExecuteHandler(Function<ExecuteRequest, CompletableFuture<ExecuteResponse>> handler);

  /**
   * Unregisters the execute request handler.
   */
  void unregisterExecuteHandler();

  /**
   * Registers a backup request callback.
   *
   * @param handler the backup request handler to register
   */
  void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler);

  /**
   * Unregisters the backup request handler.
   */
  void unregisterBackupHandler();

  /**
   * Registers a restore request callback.
   *
   * @param handler the restore request handler to register
   */
  void registerRestoreHandler(Function<RestoreRequest, CompletableFuture<RestoreResponse>> handler);

  /**
   * Unregisters the restore request handler.
   */
  void unregisterRestoreHandler();

  /**
   * Registers a close request callback.
   *
   * @param handler the close request handler to register
   */
  void registerCloseHandler(Function<CloseRequest, CompletableFuture<CloseResponse>> handler);

  /**
   * Unregisters the close request handler.
   */
  void unregisterCloseHandler();

  /**
   * Registers a metadata request callback.
   *
   * @param handler the metadata request handler to register
   */
  void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler);

  /**
   * Unregisters the metadata request handler.
   */
  void unregisterMetadataHandler();

}
