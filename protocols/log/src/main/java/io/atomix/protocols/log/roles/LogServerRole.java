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

import java.util.concurrent.CompletableFuture;

import io.atomix.protocols.log.impl.DistributedLogServerContext;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogRequest;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.atomix.protocols.log.DistributedLogServer.Role;

/**
 * Primary-backup role.
 */
public abstract class LogServerRole {
  protected final Logger log;
  private final Role role;
  protected final DistributedLogServerContext context;

  protected LogServerRole(Role role, DistributedLogServerContext context) {
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
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(AppendResponse.error()));
  }

  /**
   * Handles a consume request.
   *
   * @param request the consume request
   * @return future to be completed with the consume response
   */
  public CompletableFuture<ConsumeResponse> consume(ConsumeRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(ConsumeResponse.error()));
  }

  /**
   * Handles a reset request.
   *
   * @param request the reset request
   */
  public void reset(ResetRequest request) {
    logRequest(request);
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
