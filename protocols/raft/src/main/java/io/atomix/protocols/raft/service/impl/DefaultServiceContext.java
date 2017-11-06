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
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.service.Commit;
import io.atomix.protocols.raft.service.RaftService;
import io.atomix.protocols.raft.service.ServiceContext;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotReader;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.protocols.raft.utils.LoadMonitor;
import io.atomix.storage.buffer.Bytes;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft server state machine executor.
 */
public class DefaultServiceContext implements ServiceContext {

  private static final int LOAD_WINDOW_SIZE = 5;
  private static final int HIGH_LOAD_THRESHOLD = 50;

  private final Logger log;
  private final ServiceId serviceId;
  private final String serviceName;
  private final ServiceType serviceType;
  private final RaftService service;
  private final RaftContext raft;
  private final DefaultServiceSessions sessions;
  private final ThreadContext serviceExecutor;
  private final ThreadContext snapshotExecutor;
  private final ThreadContextFactory threadContextFactory;
  private final LoadMonitor loadMonitor;
  private final Map<Long, PendingSnapshot> pendingSnapshots = new ConcurrentSkipListMap<>();
  private long snapshotIndex;
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

  public DefaultServiceContext(
      ServiceId serviceId,
      String serviceName,
      ServiceType serviceType,
      RaftService service,
      RaftContext raft,
      ThreadContextFactory threadContextFactory) {
    this.serviceId = checkNotNull(serviceId);
    this.serviceName = checkNotNull(serviceName);
    this.serviceType = checkNotNull(serviceType);
    this.service = checkNotNull(service);
    this.raft = checkNotNull(raft);
    this.sessions = new DefaultServiceSessions(serviceId, raft.getSessions());
    this.serviceExecutor = threadContextFactory.createContext();
    this.snapshotExecutor = threadContextFactory.createContext();
    this.loadMonitor = new LoadMonitor(LOAD_WINDOW_SIZE, HIGH_LOAD_THRESHOLD, serviceExecutor);
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
   * Returns a boolean indicating whether the service is under high load.
   *
   * @return indicates whether the service is under high load
   */
  public boolean isUnderHighLoad() {
    return loadMonitor.isUnderHighLoad();
  }

  /**
   * Returns the state machine executor.
   *
   * @return The state machine executor.
   */
  public ThreadContext executor() {
    return serviceExecutor;
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
        log.debug("Closing session {}", session.sessionId());
        sessions.expireSession(session);
      }
    }
  }

  /**
   * Completes a state machine snapshot.
   */
  private void maybeCompleteSnapshot(long index) {
    if (!pendingSnapshots.isEmpty()) {
      // Compute the lowest completed index for all sessions that belong to this state machine.
      long lastCompleted = index;
      for (RaftSessionContext session : sessions.getSessions()) {
        lastCompleted = Math.min(lastCompleted, session.getLastCompleted());
      }

      for (PendingSnapshot pendingSnapshot : pendingSnapshots.values()) {
        Snapshot snapshot = pendingSnapshot.snapshot;
        if (snapshot.isPersisted()) {

          // If the lowest completed index for all sessions is greater than the snapshot index, complete the snapshot.
          if (lastCompleted >= snapshot.index()) {
            log.debug("Completing snapshot {}", snapshot.index());
            snapshot.complete();

            // Update the snapshot index to ensure we don't simply install the same snapshot.
            snapshotIndex = snapshot.index();
            pendingSnapshot.future.complete(null);
          }
        }
      }
    }
  }

  /**
   * Installs a snapshot if one exists.
   */
  private void maybeInstallSnapshot(long index) {
    // Look up the latest snapshot for this state machine.
    Snapshot snapshot = raft.getSnapshotStore().getSnapshotById(serviceId);

    // If the latest snapshot is non-null, hasn't been installed, and has an index lower than the current index, install it.
    if (snapshot != null && snapshot.index() > snapshotIndex && snapshot.index() < index) {
      log.debug("Installing snapshot {}", snapshot.index());
      try (SnapshotReader reader = snapshot.openReader()) {
        reader.skip(Bytes.LONG); // Skip the service ID
        ServiceType serviceType = ServiceType.from(reader.readString());
        String serviceName = reader.readString();
        int sessionCount = reader.readInt();
        for (int i = 0; i < sessionCount; i++) {
          SessionId sessionId = SessionId.from(reader.readLong());
          MemberId node = MemberId.from(reader.readString());
          ReadConsistency readConsistency = ReadConsistency.valueOf(reader.readString());
          long minTimeout = reader.readLong();
          long maxTimeout = reader.readLong();
          long sessionTimestamp = reader.readLong();
          RaftSessionContext session = new RaftSessionContext(
              sessionId,
              node,
              serviceName,
              serviceType,
              readConsistency,
              minTimeout,
              maxTimeout,
              this,
              raft,
              threadContextFactory);
          session.setLastUpdated(sessionTimestamp);
          session.setRequestSequence(reader.readLong());
          session.setCommandSequence(reader.readLong());
          session.setEventIndex(reader.readLong());
          session.setLastCompleted(reader.readLong());
          session.setLastApplied(snapshot.index());
          sessions.openSession(session);
        }
        currentIndex = snapshot.index();
        currentTimestamp = snapshot.timestamp().unixTimestamp();
        service.install(reader);
      } catch (Exception e) {
        log.error("Snapshot installation failed: {}", e);
      }
      snapshotIndex = snapshot.index();
    }
  }

  /**
   * Takes a snapshot of the service state.
   *
   * @param index takes a snapshot at the given index
   * @return a future to be completed once the snapshot has been taken
   */
  public CompletableFuture<Long> takeSnapshot(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {
      // If no entries have been applied to the state machine, skip the snapshot.
      if (currentIndex == 0) {
        return;
      }

      // Compute the snapshot index as the greater of the compaction index and the last index applied to this service.
      long snapshotIndex = Math.max(index, currentIndex);

      // If there's already a snapshot taken at a higher index, skip the snapshot.
      Snapshot currentSnapshot = raft.getSnapshotStore().getSnapshotById(serviceId);
      if (currentSnapshot != null && currentSnapshot.index() > index) {
        return;
      }

      log.debug("Taking snapshot {}", snapshotIndex);

      // Create a temporary in-memory snapshot buffer.
      Snapshot snapshot = raft.getSnapshotStore()
          .newTemporarySnapshot(serviceId, serviceName, snapshotIndex, WallClockTimestamp.from(currentTimestamp));

      // Add the snapshot to the pending snapshots registry.
      PendingSnapshot pendingSnapshot = new PendingSnapshot(snapshot);
      pendingSnapshots.put(snapshotIndex, pendingSnapshot);
      pendingSnapshot.future.whenComplete((r, e) -> pendingSnapshots.remove(snapshotIndex));

      // Serialize sessions to the in-memory snapshot and request a snapshot from the state machine.
      try (SnapshotWriter writer = snapshot.openWriter()) {
        writer.writeLong(serviceId.id());
        writer.writeString(serviceType.id());
        writer.writeString(serviceName);
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
      } catch (Exception e) {
        log.error("Snapshot failed: {}", e);
      }

      // Persist the snapshot to disk in a background thread before completing the snapshot future.
      snapshotExecutor.execute(() -> {
        pendingSnapshot.persist();
        future.complete(snapshotIndex);
      });
    });
    return future;
  }

  /**
   * Completes a snapshot of the service state.
   *
   * @param index the index of the snapshot to complete
   * @return a future to be completed once the snapshot has been completed
   */
  public CompletableFuture<Void> completeSnapshot(long index) {
    PendingSnapshot pendingSnapshot = pendingSnapshots.get(index);
    if (pendingSnapshot == null) {
      return CompletableFuture.completedFuture(null);
    }
    serviceExecutor.execute(() -> maybeCompleteSnapshot(index));
    return pendingSnapshot.future;
  }

  /**
   * Registers the given session.
   *
   * @param index     The index of the registration.
   * @param timestamp The timestamp of the registration.
   * @param session   The session to register.
   */
  public CompletableFuture<Long> openSession(long index, long timestamp, RaftSessionContext session) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {
      log.debug("Opening session {}", session.sessionId());

      // Update the session's timestamp to prevent it from being expired.
      session.setLastUpdated(timestamp);

      // If a snapshot exists prior to the given index and hasn't yet been installed, install the snapshot.
      maybeInstallSnapshot(index);

      // Update the state machine index/timestamp.
      tick(index, timestamp);

      // Expire sessions that have timed out.
      expireSessions(currentTimestamp);

      // Add the session to the sessions list.
      sessions.openSession(session);

      // Commit the index, causing events to be sent to clients if necessary.
      commit();

      // Complete any pending snapshots of the service state.
      maybeCompleteSnapshot(index);

      // Complete the future.
      future.complete(session.sessionId().id());
    });
    return future;
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
  public CompletableFuture<Boolean> keepAlive(long index, long timestamp, RaftSessionContext session, long commandSequence, long eventIndex) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {

      // If a snapshot exists prior to the given index and hasn't yet been installed, install the snapshot.
      maybeInstallSnapshot(index);

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
        future.complete(true);
      } else {
        future.complete(false);
      }
    });
    return future;
  }

  /**
   * Completes a keep-alive.
   *
   * @param index the keep-alive index
   * @param timestamp the keep-alive timestamp
   * @return future to be completed once the keep alive is completed
   */
  public CompletableFuture<Void> completeKeepAlive(long index, long timestamp) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {
      // Update the state machine index/timestamp.
      tick(index, timestamp);

      // Expire sessions that have timed out.
      expireSessions(currentTimestamp);

      // Commit the index, causing events to be sent to clients if necessary.
      commit();

      // Complete any pending snapshots of the service state.
      maybeCompleteSnapshot(index);

      future.complete(null);
    });
    return future;
  }

  /**
   * Keeps all sessions alive using the given timestamp.
   *
   * @param index the index of the timestamp
   * @param timestamp the timestamp with which to reset session timeouts
   * @return future to be completed once all sessions have been preserved
   */
  public CompletableFuture<Void> keepAliveSessions(long index, long timestamp) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {
      log.debug("Resetting session timeouts");

      this.currentIndex = index;
      this.currentTimestamp = Math.max(currentTimestamp, timestamp);

      for (RaftSessionContext session : sessions.getSessions()) {
        session.setLastUpdated(timestamp);
      }
    });
    return future;
  }

  /**
   * Unregister the given session.
   *
   * @param index     The index of the unregister.
   * @param timestamp The timestamp of the unregister.
   * @param session   The session to unregister.
   * @param expired   Whether the session was expired by the leader.
   */
  public CompletableFuture<Void> closeSession(long index, long timestamp, RaftSessionContext session, boolean expired) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> {
      log.debug("Closing session {}", session.sessionId());

      // Update the session's timestamp to prevent it from being expired.
      session.setLastUpdated(timestamp);

      // If a snapshot exists prior to the given index and hasn't yet been installed, install the snapshot.
      maybeInstallSnapshot(index);

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

      // Complete any pending snapshots of the service state.
      maybeCompleteSnapshot(index);

      // Complete the future.
      future.complete(null);
    });
    return future;
  }

  /**
   * Executes the given command on the state machine.
   *
   * @param index     The index of the command.
   * @param timestamp The timestamp of the command.
   * @param sequence  The command sequence number.
   * @param session   The session that submitted the command.
   * @param operation   The command to execute.
   * @return A future to be completed with the command result.
   */
  public CompletableFuture<OperationResult> executeCommand(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation) {
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> executeCommand(index, sequence, timestamp, session, operation, future));
    return future;
  }

  /**
   * Executes a command on the state machine thread.
   */
  private void executeCommand(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation, CompletableFuture<OperationResult> future) {
    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // If a snapshot exists prior to the given index and hasn't yet been installed, install the snapshot.
    maybeInstallSnapshot(index);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      future.completeExceptionally(new RaftException.UnknownSession("Unknown session: " + session.sessionId()));
      return;
    }

    // If the command's sequence number is less than the next session sequence number then that indicates that
    // we've received a command that was previously applied to the state machine. Ensure linearizability by
    // returning the cached response instead of applying it to the user defined state machine.
    if (sequence > 0 && sequence < session.nextCommandSequence()) {
      sequenceCommand(index, sequence, session, future);
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
      // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
      applyCommand(index, sequence, timestamp, operation, session, future);

      // Update the session timestamp and command sequence number. This is done in the caller's thread since all
      // timestamp/index/sequence checks are done in this thread prior to executing operations on the state machine thread.
      session.setCommandSequence(sequence);
    }
  }

  /**
   * Loads and returns a cached command result according to the sequence number.
   */
  private void sequenceCommand(long index, long sequence, RaftSessionContext session, CompletableFuture<OperationResult> future) {
    OperationResult result = session.getResult(sequence);
    if (result == null) {
      log.debug("Missing command result at index {}", index);
    }
    future.complete(result);
  }

  /**
   * Applies the given commit to the state machine.
   */
  private void applyCommand(long index, long sequence, long timestamp, RaftOperation operation, RaftSessionContext session, CompletableFuture<OperationResult> future) {
    // Ignore no-op commands.
    if (operation.id().equals(OperationId.NOOP)) {
      future.complete(OperationResult.noop(index, session.getEventIndex()));
      return;
    }

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

    // Complete the command.
    future.complete(result);
  }

  /**
   * Executes the given query on the state machine.
   *
   * @param index     The index of the query.
   * @param sequence  The query sequence number.
   * @param timestamp The timestamp of the query.
   * @param session   The session that submitted the query.
   * @param operation     The query to execute.
   * @return A future to be completed with the query result.
   */
  public CompletableFuture<OperationResult> executeQuery(long index, long sequence, long timestamp, RaftSessionContext session, RaftOperation operation) {
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    serviceExecutor.execute(() -> executeQuery(index, sequence, timestamp, session, operation, future));
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

    Commit<byte[]> commit = new DefaultCommit<>(session.getLastApplied(), operation.id(), operation.value(), session, timestamp);

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

  /**
   * Pending snapshot.
   */
  private class PendingSnapshot {
    private volatile Snapshot snapshot;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    public PendingSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    /**
     * Persists the snapshot.
     */
    void persist() {
      this.snapshot = snapshot.persist();
    }
  }
}
