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
import io.atomix.protocols.log.protocol.BackupOperation;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.Indexed;

import static io.atomix.protocols.log.DistributedLogServer.Role;

/**
 * Primary role.
 */
public class LeaderRole extends LogServerRole {
  private final Replicator replicator;

  public LeaderRole(DistributedLogServerContext context) {
    super(Role.LEADER, context);
    switch (context.replicationStrategy()) {
      case SYNCHRONOUS:
        replicator = new SynchronousReplicator(context, log);
        break;
      case ASYNCHRONOUS:
        replicator = new AsynchronousReplicator(context, log);
        break;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    logRequest(request);
    try {
      Indexed<LogEntry> entry = context.journal().writer().append(
          new LogEntry(context.currentTerm(), System.currentTimeMillis(), request.value()));
      return replicator.replicate(new BackupOperation(
          entry.index(), entry.entry().term(), entry.entry().timestamp(), entry.entry().value()))
          .thenApply(v -> AppendResponse.ok(entry.index()));
    } catch (StorageException e) {
      return CompletableFuture.completedFuture(AppendResponse.error());
    }
  }

  @Override
  public void close() {
    replicator.close();
  }
}
