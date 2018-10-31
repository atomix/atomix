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

import io.atomix.protocols.log.DistributedLogServer.Role;
import io.atomix.protocols.log.impl.LogServerContext;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Backup role.
 */
public class FollowerRole extends LogServerRole {
  public FollowerRole(LogServerContext service) {
    super(Role.FOLLOWER, service);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    logRequest(request);

    // If the term is greater than the node's current term, update the term.
    if (request.term() > context.currentTerm()) {
      context.resetTerm(request.term(), request.leader());
    }
    // If the term is less than the node's current term, ignore the backup message.
    else if (request.term() < context.currentTerm()) {
      return CompletableFuture.completedFuture(BackupResponse.error());
    }

    // TODO: Append entries
    return CompletableFuture.completedFuture(logResponse(BackupResponse.ok()));
  }
}
