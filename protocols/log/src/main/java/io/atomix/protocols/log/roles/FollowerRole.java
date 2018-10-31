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

import io.atomix.protocols.log.DistributedLogServer.Role;
import io.atomix.protocols.log.impl.DistributedLogServerContext;
import io.atomix.protocols.log.protocol.BackupOperation;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.JournalReader;
import io.atomix.storage.journal.JournalWriter;

/**
 * Backup role.
 */
public class FollowerRole extends LogServerRole {
  public FollowerRole(DistributedLogServerContext service) {
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

    JournalWriter<LogEntry> writer = context.writer();
    JournalReader<LogEntry> reader = context.reader();

    // Iterate through all operations in the batch and append entries.
    for (BackupOperation operation : request.batch()) {

      // If the reader's next index does not align with the operation index, reset the reader.
      if (reader.getNextIndex() != operation.index()) {
        reader.reset(operation.index());
      }

      // If the reader has no next entry, append the entry to the journal.
      if (!reader.hasNext()) {
        try {
          writer.append(new LogEntry(operation.term(), operation.timestamp(), operation.value()));
        } catch (StorageException e) {
          return CompletableFuture.completedFuture(BackupResponse.error());
        }
      }
      // If the next entry's term does not match the operation term, append the entry to the journal.
      else if (reader.next().entry().term() != operation.term()) {
        writer.truncate(operation.index());
        try {
          writer.append(new LogEntry(operation.term(), operation.timestamp(), operation.value()));
        } catch (StorageException e) {
          return CompletableFuture.completedFuture(BackupResponse.error());
        }
      }
    }
    return CompletableFuture.completedFuture(logResponse(BackupResponse.ok()));
  }
}
