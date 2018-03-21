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

package io.atomix.protocols.raft.service.impl;

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.impl.OperationResult;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.protocols.raft.service.ServiceContext;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class DefaultServiceContext implements ServiceContext {
  private final Logger log;
  private final ServiceId serviceId;
  private final String serviceName;
  private final ServiceType serviceType;
  private final ServiceRevision revision;
  private final RaftService service;
  private final RaftContext raft;
  private final DefaultServiceSessions sessions;
  private final ThreadContextFactory threadContextFactory;
  private long currentIndex;
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
  private DefaultServiceContext nextRevision;
  private boolean locked = false;

  public DefaultServiceContext(
      ServiceId serviceId,
      String serviceName,
      ServiceType serviceType,
      ServiceRevision revision,
      RaftService service,
      RaftContext raft,
      ThreadContextFactory threadContextFactory) {
    this.serviceId = checkNotNull(serviceId);
    this.serviceName = checkNotNull(serviceName);
    this.serviceType = checkNotNull(serviceType);
    this.revision = checkNotNull(revision);
    this.service = checkNotNull(service);
    this.raft = checkNotNull(raft);
    this.sessions = new DefaultServiceSessions(serviceId, raft.getSessions());
    this.threadContextFactory = threadContextFactory;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftService.class)
        .addValue(serviceId)
        .add("type", serviceType)
        .add("name", serviceName)
        .build());
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    sessions.addListener(service);
    service.init(this);
  }

  @Override
  public ServiceId serviceId() {
    return serviceId;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public ServiceType serviceType() {
    return serviceType;
  }

  @Override
  public ServiceRevision revision() {
    return revision;
  }

  /**
   * Returns the underlying state machine.
   *
   * @return the underlying state machine
   */
  public RaftService service() {
    return service;
  }

  @Override
  public long currentIndex() {
    return currentIndex;
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

  @Override
  public RaftSessions sessions() {
    return sessions;
  }

  /**
   * Sets the next revision to which to propagate writes.
   *
   * @param revision the revision to which to propagate writes
   */
  public void setNextRevision(DefaultServiceContext revision) {
    this.nextRevision = revision;
  }

  /**
   * Returns whether the context is locked.
   *
   * @return whether the context is locked
   */
  public boolean locked() {
    return locked;
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
    for (RaftSessionContext session : sessions.getSessions()) {
      if (session.isTimedOut(timestamp)) {
        log.debug("Session expired in {} milliseconds: {}", timestamp - session.getLastUpdated(), session);
        sessions.expireSession(session);
      }
    }
  }

  /**
   * Installs a snapshot.
   */
  public void installSnapshot(SnapshotReader reader) {
    log.debug("Installing snapshot {}", reader.snapshot().index());
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
      RaftSessionContext session = sessions.addSession(new RaftSessionContext(
          sessionId,
          node,
          serviceName,
          serviceType,
          readConsistency,
          minTimeout,
          maxTimeout,
          sessionTimestamp,
          this,
          raft,
          threadContextFactory));

      session.setRequestSequence(reader.readLong());
      session.setCommandSequence(reader.readLong());
      session.setEventIndex(reader.readLong());
      session.setLastCompleted(reader.readLong());
      session.setLastApplied(reader.snapshot().index());
      session.setLastUpdated(sessionTimestamp);
      sessions.openSession(session);
    }
    currentIndex = reader.snapshot().index();
    currentTimestamp = reader.snapshot().timestamp().unixTimestamp();
    service.install(reader);
  }

  /**
   * Takes a snapshot of the service state.
   */
  public void takeSnapshot(SnapshotWriter writer) {
    log.debug("Taking snapshot {}", writer.snapshot().index());

    // Serialize sessions to the in-memory snapshot and request a snapshot from the state machine.
    writer.writeInt(sessions.getSessions().size());
    for (RaftSessionContext session : sessions.getSessions()) {
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
    service.snapshot(writer);
  }

  /**
   * Registers the given session.
   *
   * @param index     The index of the registration.
   * @param timestamp The timestamp of the registration.
   * @param session   The session to register.
   */
  public long openSession(long index, long timestamp, RaftSessionContext session) {
    log.debug("Opening session {}", session.sessionId());

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Add the session to the sessions list.
    sessions.openSession(session);

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
  public boolean keepAlive(long index, long timestamp, RaftSessionContext session, long commandSequence, long eventIndex) {
    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // The session may have been closed by the time this update was executed on the service thread.
    if (session.getState() != RaftSession.State.CLOSED) {
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

    for (RaftSessionContext session : sessions.getSessions()) {
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
  public void closeSession(long index, long timestamp, RaftSessionContext session, boolean expired) {
    log.debug("Closing session {}", session.sessionId());

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Remove the session from the sessions list.
    if (expired) {
      sessions.expireSession(session);
    } else {
      sessions.closeSession(session);
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
  public OperationResult executeCommand(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation) {
    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

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
      return sequenceCommand(index, sequence, timestamp, session);
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
  private OperationResult sequenceCommand(long index, long sequence, long timestamp, RaftSessionContext session) {
    // Update the state machine index/timestamp.
    tick(index, timestamp);

    OperationResult result = session.getResult(sequence);
    if (result == null) {
      log.debug("Missing command result at index {}", index);
    }
    return result;
  }

  /**
   * Applies the given commit to the state machine.
   */
  private OperationResult applyCommand(long index, long sequence, long timestamp, RaftOperation operation, RaftSessionContext session) {
    // If a later revision of this service exists, determine whether the command needs to be propagated.
    if (nextRevision != null) {
      switch (nextRevision.revision().propagationStrategy()) {
        case VERSION:
          return OperationResult.failed(
              index,
              session.getEventIndex(),
              new RaftException.ReadOnly(serviceName() + " revision " + revision.revision() + " is read-only"));
        case PROPAGATE:
          locked = true;
          try {
            nextRevision.applyCommand(index, sequence, timestamp, operation, session);
          } finally {
            locked = false;
          }
          break;
        default:
          break;
      }
    }

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    Commit<byte[]> commit = new DefaultCommit<>(index, operation.id(), operation.value(), session, timestamp);

    long eventIndex = session.getEventIndex();

    OperationResult result;
    try {
      // Execute the state machine operation and get the result.
      byte[] output = service.apply(commit);

      // Store the result for linearizability and complete the command.
      result = OperationResult.succeeded(index, eventIndex, output);
    } catch (Exception e) {
      // If an exception occurs during execution of the command, store the exception.
      result = OperationResult.failed(index, eventIndex, e);
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
  public CompletableFuture<OperationResult> executeQuery(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation) {
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    executeQuery(index, sequence, timestamp, session, operation, future);
    return future;
  }

  /**
   * Executes a query on the state machine thread.
   */
  private void executeQuery(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation, CompletableFuture<OperationResult> future) {
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
  private void sequenceQuery(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation, CompletableFuture<OperationResult> future) {
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
  private void indexQuery(long index, long timestamp, RaftSessionContext session, RaftOperation operation, CompletableFuture<OperationResult> future) {
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
  private void applyQuery(long timestamp, RaftSessionContext session, RaftOperation operation, CompletableFuture<OperationResult> future) {
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
      result = OperationResult.succeeded(currentIndex, eventIndex, service.apply(commit));
    } catch (Exception e) {
      result = OperationResult.failed(currentIndex, eventIndex, e);
    }
    future.complete(result);
  }

  /**
   * Commits the application of a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  private void commit() {
    long index = this.currentIndex;
    for (RaftSessionContext session : sessions.getSessions()) {
      session.commit(index);
    }
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("server", raft.getName())
        .add("type", serviceType)
        .add("name", serviceName)
        .add("id", serviceId)
        .toString();
  }
}
