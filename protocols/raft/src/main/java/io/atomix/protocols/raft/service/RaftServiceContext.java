/*
 * Copyright 2015-present Open Networking Foundation
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

package io.atomix.protocols.raft.service;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.impl.DefaultBackupInput;
import io.atomix.primitive.service.impl.DefaultBackupOutput;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.PrimitiveSession;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.impl.OperationResult;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionRegistry;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.storage.buffer.Bytes;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class RaftServiceContext implements ServiceContext {
  private final Logger log;
  private final PrimitiveId primitiveId;
  private final String serviceName;
  private final PrimitiveType primitiveType;
  private final ServiceConfig config;
  private final PrimitiveService service;
  private final RaftContext raft;
  private final RaftSessionRegistry sessions;
  private final ThreadContextFactory threadContextFactory;
  private long currentIndex;
  private PrimitiveSession currentSession;
  private long currentTimestamp;
  private OperationType currentOperation;
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp getTime() {
      return new LogicalTimestamp(currentIndex);
    }
  };
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp getTime() {
      return new WallClockTimestamp(currentTimestamp);
    }
  };

  public RaftServiceContext(
      PrimitiveId primitiveId,
      String serviceName,
      PrimitiveType primitiveType,
      ServiceConfig config,
      PrimitiveService service,
      RaftContext raft,
      ThreadContextFactory threadContextFactory) {
    this.primitiveId = checkNotNull(primitiveId);
    this.serviceName = checkNotNull(serviceName);
    this.primitiveType = checkNotNull(primitiveType);
    this.config = checkNotNull(config);
    this.service = checkNotNull(service);
    this.raft = checkNotNull(raft);
    this.sessions = raft.getSessions();
    this.threadContextFactory = threadContextFactory;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(primitiveId)
        .add("type", primitiveType)
        .add("name", serviceName)
        .build());
    service.init(this);
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends ServiceConfig> C serviceConfig() {
    return (C) config;
  }

  public Serializer serializer() {
    return service.serializer();
  }

  @Override
  public long currentIndex() {
    return currentIndex;
  }

  @Override
  public PrimitiveSession currentSession() {
    return currentSession;
  }

  @Override
  public OperationType currentOperation() {
    return currentOperation;
  }

  @Override
  public LogicalClock logicalClock() {
    return logicalClock;
  }

  @Override
  public WallClock wallClock() {
    return wallClock;
  }

  /**
   * Sets the current state machine operation type.
   *
   * @param operation the current state machine operation type
   */
  private void setOperation(OperationType operation) {
    this.currentOperation = operation;
  }

  /**
   * Executes scheduled callbacks based on the provided time.
   */
  private void tick(long index, long timestamp) {
    this.currentIndex = index;
    this.currentTimestamp = Math.max(currentTimestamp, timestamp);

    // Set the current operation type to COMMAND to allow events to be sent.
    setOperation(OperationType.COMMAND);

    service.tick(WallClockTimestamp.from(timestamp));
  }

  /**
   * Expires sessions that have timed out.
   */
  private void expireSessions(long timestamp) {
    // Iterate through registered sessions.
    for (RaftSession session : sessions.getSessions(primitiveId)) {
      if (session.isTimedOut(timestamp)) {
        log.debug("Session expired in {} milliseconds: {}", timestamp - session.getLastUpdated(), session);
        session = sessions.removeSession(session.sessionId());
        if (session != null) {
          session.expire();
          service.expire(session.sessionId());
        }
      }
    }
  }

  /**
   * Installs a snapshot.
   */
  public void installSnapshot(SnapshotReader reader) {
    log.debug("Installing snapshot {}", reader.snapshot().index());
    reader.skip(Bytes.LONG); // Skip the service ID
    PrimitiveType primitiveType = raft.getPrimitiveTypes().get(reader.readString());
    String serviceName = reader.readString();
    int sessionCount = reader.readInt();
    for (int i = 0; i < sessionCount; i++) {
      SessionId sessionId = SessionId.from(reader.readLong());
      MemberId node = MemberId.from(reader.readString());
      ReadConsistency readConsistency = ReadConsistency.valueOf(reader.readString());
      long minTimeout = reader.readLong();
      long maxTimeout = reader.readLong();
      long sessionTimestamp = reader.readLong();

      // Only create a new session if one does not already exist. This is necessary to ensure only a single session
      // is ever opened and exposed to the state machine.
      RaftSession session = raft.getSessions().addSession(new RaftSession(
          sessionId,
          node,
          serviceName,
          primitiveType,
          readConsistency,
          minTimeout,
          maxTimeout,
          sessionTimestamp,
          service.serializer(),
          this,
          raft,
          threadContextFactory));

      session.setRequestSequence(reader.readLong());
      session.setCommandSequence(reader.readLong());
      session.setEventIndex(reader.readLong());
      session.setLastCompleted(reader.readLong());
      session.setLastApplied(reader.snapshot().index());
      session.setLastUpdated(sessionTimestamp);
      service.register(sessions.addSession(session));
    }
    currentIndex = reader.snapshot().index();
    currentTimestamp = reader.snapshot().timestamp().unixTimestamp();
    service.restore(new DefaultBackupInput(reader, service.serializer()));
  }

  /**
   * Takes a snapshot of the service state.
   */
  public void takeSnapshot(SnapshotWriter writer) {
    log.debug("Taking snapshot {}", writer.snapshot().index());

    // Serialize sessions to the in-memory snapshot and request a snapshot from the state machine.
    writer.writeLong(primitiveId.id());
    writer.writeString(primitiveType.id());
    writer.writeString(serviceName);
    writer.writeInt(sessions.getSessions().size());
    for (RaftSession session : sessions.getSessions()) {
      writer.writeLong(session.sessionId().id());
      writer.writeString(session.memberId().id());
      writer.writeString(session.readConsistency().name());
      writer.writeLong(session.minTimeout());
      writer.writeLong(session.maxTimeout());
      writer.writeLong(session.getLastUpdated());
      writer.writeLong(session.getRequestSequence());
      writer.writeLong(session.getCommandSequence());
      writer.writeLong(session.getEventIndex());
      writer.writeLong(session.getLastCompleted());
    }
    service.backup(new DefaultBackupOutput(writer, service.serializer()));
  }

  /**
   * Registers the given session.
   *
   * @param index     The index of the registration.
   * @param timestamp The timestamp of the registration.
   * @param session   The session to register.
   */
  public long openSession(long index, long timestamp, RaftSession session) {
    log.debug("Opening session {}", session.sessionId());

    // Update the state machine index/ timestamp .
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Add the session to the sessions list.
    session.open();
    service.register(sessions.addSession(session));

    // Commit the index, causing events to be sent to clients if necessary.
    commit();

    // Complete the future.
    return session.sessionId().id();
  }

  /**
   * Keeps the given session alive.
   *
   * @param index           The index of the keep-alive.
   * @param timestamp       The timestamp of the keep-alive.
   * @param session         The session to keep-alive.
   * @param commandSequence The session command sequence number.
   * @param eventIndex      The session event index.
   */
  public boolean keepAlive(long index, long timestamp, RaftSession session, long commandSequence, long eventIndex) {
    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // The session may have been closed by the time this update was executed on the service thread.
    if (session.getState() != PrimitiveSession.State.CLOSED) {
      // Update the session's timestamp to prevent it from being expired.
      session.setLastUpdated(timestamp);

      // Clear results cached in the session.
      session.clearResults(commandSequence);

      // Resend missing events starting from the last received event index.
      session.resendEvents(eventIndex);

      // Update the session's request sequence number. The command sequence number will be applied
      // iff the existing request sequence number is less than the command sequence number. This must
      // be applied to ensure that request sequence numbers are reset after a leader change since leaders
      // track request sequence numbers in local memory.
      session.resetRequestSequence(commandSequence);

      // Update the sessions' command sequence number. The command sequence number will be applied
      // iff the existing sequence number is less than the keep-alive command sequence number. This should
      // not be the case under normal operation since the command sequence number in keep-alive requests
      // represents the highest sequence for which a client has received a response (the command has already
      // been completed), but since the log compaction algorithm can exclude individual entries from replication,
      // the command sequence number must be applied for keep-alive requests to reset the sequence number in
      // the event the last command for the session was cleaned/compacted from the log.
      session.setCommandSequence(commandSequence);

      // Complete the future.
      return true;
    } else {
      return false;
    }
  }

  /**
   * Completes a keep-alive.
   *
   * @param index     the keep-alive index
   * @param timestamp the keep-alive timestamp
   */
  public void completeKeepAlive(long index, long timestamp) {
    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Commit the index, causing events to be sent to clients if necessary.
    commit();
  }

  /**
   * Keeps all sessions alive using the given timestamp.
   *
   * @param index     the index of the timestamp
   * @param timestamp the timestamp with which to reset session timeouts
   */
  public void keepAliveSessions(long index, long timestamp) {
    log.debug("Resetting session timeouts");

    this.currentIndex = index;
    this.currentTimestamp = Math.max(currentTimestamp, timestamp);

    for (RaftSession session : sessions.getSessions(primitiveId)) {
      session.setLastUpdated(timestamp);
    }
  }

  /**
   * Unregister the given session.
   *
   * @param index     The index of the unregister.
   * @param timestamp The timestamp of the unregister.
   * @param session   The session to unregister.
   * @param expired   Whether the session was expired by the leader.
   */
  public void closeSession(long index, long timestamp, RaftSession session, boolean expired) {
    log.debug("Closing session {}", session.sessionId());

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Remove the session from the sessions list.
    if (expired) {
      session = sessions.removeSession(session.sessionId());
      if (session != null) {
        session.expire();
        service.expire(session.sessionId());
      }
    } else {
      session = sessions.removeSession(session.sessionId());
      if (session != null) {
        session.close();
        service.close(session.sessionId());
      }
    }

    // Commit the index, causing events to be sent to clients if necessary.
    commit();
  }

  /**
   * Executes the given command on the state machine.
   *
   * @param index     The index of the command.
   * @param timestamp The timestamp of the command.
   * @param sequence  The command sequence number.
   * @param session   The session that submitted the command.
   * @param operation The command to execute.
   * @return A future to be completed with the command result.
   */
  public OperationResult executeCommand(long index, long sequence, long timestamp, RaftSession session, PrimitiveOperation operation) {
    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Session not open: {}", session);
      throw new RaftException.UnknownSession("Unknown session: " + session.sessionId());
    }

    // If the command's sequence number is less than the next session sequence number then that indicates that
    // we've received a command that was previously applied to the state machine. Ensure linearizability by
    // returning the cached response instead of applying it to the user defined state machine.
    if (sequence > 0 && sequence < session.nextCommandSequence()) {
      log.trace("Returning cached result for command with sequence number {} < {}", sequence, session.nextCommandSequence());
      return sequenceCommand(index, sequence, session);
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
      // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
      return applyCommand(index, sequence, timestamp, operation, session);
    }
  }

  /**
   * Loads and returns a cached command result according to the sequence number.
   */
  private OperationResult sequenceCommand(long index, long sequence, RaftSession session) {
    OperationResult result = session.getResult(sequence);
    if (result == null) {
      log.debug("Missing command result at index {}", index);
    }
    return result;
  }

  /**
   * Applies the given commit to the state machine.
   */
  private OperationResult applyCommand(long index, long sequence, long timestamp, PrimitiveOperation operation, RaftSession session) {
    Commit<byte[]> commit = new DefaultCommit<>(index, operation.id(), operation.value(), session, timestamp);

    long eventIndex = session.getEventIndex();

    OperationResult result;
    try {
      currentSession = session;

      // Execute the state machine operation and get the result.
      byte[] output = service.apply(commit);

      // Store the result for linearizability and complete the command.
      result = OperationResult.succeeded(index, eventIndex, output);
    } catch (Exception e) {
      // If an exception occurs during execution of the command, store the exception.
      result = OperationResult.failed(index, eventIndex, e);
    } finally {
      currentSession = null;
    }

    // Once the operation has been applied to the state machine, commit events published by the command.
    // The state machine context will build a composite future for events published to all sessions.
    commit();

    // Register the result in the session to ensure retries receive the same output for the command.
    session.registerResult(sequence, result);

    // Update the session timestamp and command sequence number.
    session.setCommandSequence(sequence);

    // Complete the command.
    return result;
  }

  /**
   * Executes the given query on the state machine.
   *
   * @param index     The index of the query.
   * @param sequence  The query sequence number.
   * @param timestamp The timestamp of the query.
   * @param session   The session that submitted the query.
   * @param operation The query to execute.
   * @return A future to be completed with the query result.
   */
  public CompletableFuture<OperationResult> executeQuery(long index, long sequence, long timestamp, RaftSession session, PrimitiveOperation operation) {
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    executeQuery(index, sequence, timestamp, session, operation, future);
    return future;
  }

  /**
   * Executes a query on the state machine thread.
   */
  private void executeQuery(long index, long sequence, long timestamp, RaftSession session, PrimitiveOperation operation, CompletableFuture<OperationResult> future) {
    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Inactive session: " + session.sessionId());
      future.completeExceptionally(new RaftException.UnknownSession("Unknown session: " + session.sessionId()));
      return;
    }

    // Otherwise, sequence the query.
    sequenceQuery(index, sequence, timestamp, session, operation, future);
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(long index, long sequence, long timestamp, RaftSession session, PrimitiveOperation operation, CompletableFuture<OperationResult> future) {
    // If the query's sequence number is greater than the session's current sequence number, queue the request for
    // handling once the state machine is caught up.
    long commandSequence = session.getCommandSequence();
    if (sequence > commandSequence) {
      log.trace("Registering query with sequence number " + sequence + " > " + commandSequence);
      session.registerSequenceQuery(sequence, () -> indexQuery(index, timestamp, session, operation, future));
    } else {
      indexQuery(index, timestamp, session, operation, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(long index, long timestamp, RaftSession session, PrimitiveOperation operation, CompletableFuture<OperationResult> future) {
    // If the query index is greater than the session's last applied index, queue the request for handling once the
    // state machine is caught up.
    if (index > currentIndex) {
      log.trace("Registering query with index " + index + " > " + currentIndex);
      session.registerIndexQuery(index, () -> applyQuery(timestamp, session, operation, future));
    } else {
      applyQuery(timestamp, session, operation, future);
    }
  }

  /**
   * Applies a query to the state machine.
   */
  private void applyQuery(long timestamp, RaftSession session, PrimitiveOperation operation, CompletableFuture<OperationResult> future) {
    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Inactive session: " + session.sessionId());
      future.completeExceptionally(new RaftException.UnknownSession("Unknown session: " + session.sessionId()));
      return;
    }

    // Set the current operation type to QUERY to prevent events from being sent to clients.
    setOperation(OperationType.QUERY);

    Commit<byte[]> commit = new DefaultCommit<>(currentIndex, operation.id(), operation.value(), session, timestamp);

    long eventIndex = session.getEventIndex();

    OperationResult result;
    try {
      currentSession = session;
      result = OperationResult.succeeded(currentIndex, eventIndex, service.apply(commit));
    } catch (Exception e) {
      result = OperationResult.failed(currentIndex, eventIndex, e);
    } finally {
      currentSession = null;
    }
    future.complete(result);
  }

  /**
   * Commits the application of a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  private void commit() {
    long index = this.currentIndex;
    for (RaftSession session : sessions.getSessions(primitiveId)) {
      session.commit(index);
    }
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("server", raft.getName())
        .add("type", primitiveType)
        .add("name", serviceName)
        .add("id", primitiveId)
        .toString();
  }
}
