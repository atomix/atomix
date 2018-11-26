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
package io.atomix.protocols.log.roles;

import io.atomix.protocols.log.impl.LogServerContext;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.LogRequest;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.ReadRequest;
import io.atomix.protocols.log.protocol.ReadResponse;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.atomix.protocols.log.DistributedLogServer.Role;

/**
 * Primary-backup role.
 */
public abstract class LogServerRole {
  protected final Logger log;
  private final Role role;
  protected final LogServerContext context;

  protected LogServerRole(Role role, LogServerContext context) {
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
  protected final <R extends LogRequest> R logRequest(R request) {
    log.trace("Received {}", request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends LogResponse> R logResponse(R response) {
    log.trace("Sending {}", response);
    return response;
  }

  /**
   * Handles an append response.
   *
   * @param request the append request
   * @return future to be completed with the append response
   */
  public CompletableFuture<AppendResponse> execute(AppendRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(AppendResponse.error()));
  }

  /**
   * Handles a read request.
   *
   * @param request the read request
   * @return future to be completed with the read response
   */
  public CompletableFuture<ReadResponse> restore(ReadRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(ReadResponse.error()));
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
