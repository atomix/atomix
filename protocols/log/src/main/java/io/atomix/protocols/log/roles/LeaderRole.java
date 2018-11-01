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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.log.LogRecord;
import io.atomix.protocols.log.impl.DistributedLogServerContext;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupOperation;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.Indexed;
import io.atomix.storage.journal.JournalReader;

import static io.atomix.protocols.log.DistributedLogServer.Role;

/**
 * Primary role.
 */
public class LeaderRole extends LogServerRole {
  private final Replicator replicator;
  private final Map<ConsumerKey, ConsumerSender> consumers = Maps.newHashMap();

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
          .thenApply(v -> {
            consumers.values().forEach(consumer -> consumer.next());
            return logResponse(AppendResponse.ok(entry.index()));
          });
    } catch (StorageException e) {
      return CompletableFuture.completedFuture(logResponse(AppendResponse.error()));
    }
  }

  @Override
  public CompletableFuture<ConsumeResponse> consume(ConsumeRequest request) {
    logRequest(request);
    JournalReader<LogEntry> reader = context.journal().openReader(request.index(), JournalReader.Mode.COMMITS);
    ConsumerSender consumer = new ConsumerSender(request.memberId(), request.subject(), reader);
    consumers.put(new ConsumerKey(request.memberId(), request.subject()), consumer);
    consumer.next();
    return CompletableFuture.completedFuture(logResponse(ConsumeResponse.ok()));
  }

  @Override
  public void reset(ResetRequest request) {
    logRequest(request);
    ConsumerSender consumer = consumers.get(new ConsumerKey(request.memberId(), request.subject()));
    if (consumer != null) {
      consumer.reset(request.index());
    }
  }

  @Override
  public void close() {
    replicator.close();
    consumers.values().forEach(consumer -> consumer.close());
  }

  /**
   * Consumer sender.
   */
  class ConsumerSender {
    private final MemberId memberId;
    private final String subject;
    private final JournalReader<LogEntry> reader;
    private boolean open = true;

    ConsumerSender(MemberId memberId, String subject, JournalReader<LogEntry> reader) {
      this.memberId = memberId;
      this.subject = subject;
      this.reader = reader;
    }

    /**
     * Resets the consumer to the given index.
     *
     * @param index the index to which to reset the consumer
     */
    void reset(long index) {
      reader.reset(index);
      next();
    }

    /**
     * Sends the next batch to the consumer.
     */
    void next() {
      if (!open) {
        return;
      }
      context.threadContext().execute(() -> {
        if (reader.hasNext()) {
          Indexed<LogEntry> entry = reader.next();
          LogRecord record = new LogRecord(entry.index(), entry.entry().timestamp(), entry.entry().value());
          boolean reset = reader.getFirstIndex() == entry.index();
          RecordsRequest request = RecordsRequest.request(record, reset);
          log.trace("Sending {} to {} at {}", request, memberId, subject);
          context.protocol().produce(memberId, subject, request);
          next();
        }
      });
    }

    /**
     * Closes the consumer.
     */
    void close() {
      reader.close();
      open = false;
    }
  }

  /**
   * Consumer key.
   */
  class ConsumerKey {
    private final MemberId memberId;
    private final String subject;

    ConsumerKey(MemberId memberId, String subject) {
      this.memberId = memberId;
      this.subject = subject;
    }

    @Override
    public int hashCode() {
      return Objects.hash(memberId, subject);
    }

    @Override
    public boolean equals(Object object) {
      if (object instanceof ConsumerKey) {
        ConsumerKey that = (ConsumerKey) object;
        return this.memberId.equals(that.memberId) && this.subject.equals(that.subject);
      }
      return false;
    }
  }
}
