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
package io.atomix.protocols.backup.roles;

import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupRequest;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primary-backup role.
 */
public abstract class PrimaryBackupRole {
  protected final Logger log;
  private final Role role;
  protected final PrimaryBackupServiceContext context;

  protected PrimaryBackupRole(Role role, PrimaryBackupServiceContext context) {
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(getClass())
        .addValue(context.serverName())
        .add("role", role)
        .build());
    this.role = role;
    this.context = context;
  }

  /**
   * Returns the role type.
   *
   * @return the role type
   */
  public Role role() {
    return role;
  }

  /**
   * Logs a request.
   */
  protected final <R extends PrimaryBackupRequest> R logRequest(R request) {
    log.trace("Received {}", request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends PrimaryBackupResponse> R logResponse(R response) {
    log.trace("Sending {}", response);
    return response;
  }

  /**
   * Handles an execute response.
   *
   * @param request the execute request
   * @return future to be completed with the execute response
   */
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(ExecuteResponse.error()));
  }

  /**
   * Handles a backup request.
   *
   * @param request the backup request
   * @return future to be completed with the backup response
   */
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(BackupResponse.error()));
  }

  /**
   * Handles a restore request.
   *
   * @param request the restore request
   * @return future to be completed with the restore response
   */
  public CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(RestoreResponse.error()));
  }

  /**
   * Expires the given session.
   *
   * @param session the session to expire
   * @return a future to be completed once the session has been expires
   */
  public CompletableFuture<Void> expire(PrimaryBackupSession session) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Closes the given session.
   *
   * @param session the session to close
   * @return a future to be completed once the session has been closed
   */
  public CompletableFuture<Void> close(PrimaryBackupSession session) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Closes the role.
   */
  public void close() {
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("role", role)
        .toString();
  }
}
