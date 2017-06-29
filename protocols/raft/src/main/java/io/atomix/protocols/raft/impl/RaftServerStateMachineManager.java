/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.RaftStateMachine;
import io.atomix.protocols.raft.ServiceName;
import io.atomix.protocols.raft.ServiceType;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.error.InternalException;
import io.atomix.protocols.raft.error.UnknownSessionException;
import io.atomix.protocols.raft.error.UnknownStateMachineException;
import io.atomix.protocols.raft.session.RaftSessionMetadata;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.session.impl.RaftSessionManager;
import io.atomix.protocols.raft.storage.log.RaftLog;
import io.atomix.protocols.raft.storage.log.RaftLogReader;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.StateMachineId;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadPoolContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Internal server state machine.
 * <p>
 * The internal state machine handles application of commands to the user provided {@link RaftStateMachine}
 * and keeps track of internal state like sessions and the various indexes relevant to log compaction.
 */
public class RaftServerStateMachineManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftServerStateMachineManager.class);
  private static final long COMPACT_INTERVAL_MILLIS = 1000 * 60;

  private final RaftServerContext state;
  private final ScheduledExecutorService threadPool;
  private final ThreadContext threadContext;
  private final RaftLog log;
  private final RaftLogReader reader;
  private final RaftSessionManager sessionManager = new RaftSessionManager();
  private final Map<String, RaftServerServiceContext> stateMachines = new HashMap<>();
  private volatile long lastApplied;

  public RaftServerStateMachineManager(RaftServerContext state, ScheduledExecutorService threadPool, ThreadContext threadContext) {
    this.state = checkNotNull(state, "state cannot be null");
    this.log = state.getLog();
    this.reader = log.openReader(1, RaftLogReader.Mode.COMMITS);
    this.threadPool = threadPool;
    this.threadContext = threadContext;
    threadContext.schedule(Duration.ofMillis(COMPACT_INTERVAL_MILLIS), this::compactLog);
  }

  /**
   * Returns the session manager.
   *
   * @return The session manager.
   */
  public RaftSessionManager getSessions() {
    return sessionManager;
  }

  /**
   * Returns the last applied index.
   *
   * @return The last applied index.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last applied index.
   * <p>
   * The last applied index is updated *after* each time a non-query entry is applied to the state machine.
   *
   * @param lastApplied The last applied index.
   */
  private void setLastApplied(long lastApplied) {
    // lastApplied should be either equal to or one greater than this.lastApplied.
    if (lastApplied > this.lastApplied) {
      checkArgument(lastApplied == this.lastApplied + 1, "lastApplied must be sequential");
      this.lastApplied = lastApplied;
    } else {
      checkArgument(lastApplied == this.lastApplied, "lastApplied cannot be decreased");
    }
  }

  /**
   * Applies all commits up to the given index.
   * <p>
   * Calls to this method are assumed not to expect a result. This allows some optimizations to be
   * made internally since linearizable events don't have to be waited to complete the command.
   *
   * @param index The index up to which to apply commits.
   */
  public void applyAll(long index) {
    if (!log.isOpen()) {
      return;
    }

    // Only apply valid indices.
    if (index > 0) {
      threadContext.execute(() -> {
        // Don't attempt to apply indices that have already been applied.
        if (index > lastApplied) {
          applyIndex(index);
        }
      });
    }
  }

  /**
   * Applies the entry at the given index to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events
   * triggered by the application of the command at the given index will be awaited before completing
   * the returned future.
   *
   * @param index The index to apply.
   * @return A completable future to be completed once the commit has been applied.
   */
  public <T> CompletableFuture<T> apply(long index) {
    ComposableFuture<T> future = new ComposableFuture<>();
    threadContext.execute(() -> this.<T>applyIndex(index).whenComplete(future));
    return future;
  }

  /**
   * Applies the entry at the given index.
   */
  private <T> CompletableFuture<T> applyIndex(long index) {
    threadContext.checkThread();

    reader.getLock().lock();

    try {
      // Apply entries prior to this entry.
      while (reader.hasNext()) {
        long nextIndex = reader.getNextIndex();

        // If the next index is less than or equal to the given index, read and apply the entry.
        if (nextIndex < index) {
          Indexed<RaftLogEntry> entry = reader.next();
          applyEntry(entry);
          setLastApplied(entry.index());
        }
        // If the next index is equal to the applied index, apply it and return the result.
        else if (nextIndex == index) {
          // Read the entry from the log. If the entry is non-null then apply it, otherwise
          // simply update the last applied index and return a null result.
          try {
            Indexed<RaftLogEntry> entry = reader.next();
            if (entry.index() != index) {
              throw new IllegalStateException("inconsistent index applying entry " + index + ": " + entry);
            }
            return applyEntry(entry);
          } finally {
            setLastApplied(index);
          }
        }
        // If the applied index has been passed, return a null result.
        else {
          setLastApplied(index);
          return CompletableFuture.completedFuture(null);
        }
      }
      return CompletableFuture.completedFuture(null);
    } finally {
      reader.getLock().unlock();
    }
  }

  /**
   * Applies an entry to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events
   * triggered by the application of the given entry will be awaited before completing the returned future.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public <T> CompletableFuture<T> apply(Indexed<? extends RaftLogEntry> entry) {
    ThreadContext context = ThreadContext.currentContextOrThrow();
    return applyEntry(entry, context);
  }

  /**
   * Applies an entry to the state machine, returning a future that's completed on the given context.
   */
  private <T> CompletableFuture<T> applyEntry(Indexed<? extends RaftLogEntry> entry, ThreadContext context) {
    ComposableFuture<T> future = new ComposableFuture<T>();
    BiConsumer<T, Throwable> callback = (result, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(result));
      } else {
        context.execute(() -> future.completeExceptionally(error));
      }
    };
    threadContext.execute(() -> this.<T>applyEntry(entry).whenComplete(callback));
    return future;
  }

  /**
   * Applies an entry to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events
   * triggered by the application of the given entry will be awaited before completing the returned future.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> applyEntry(Indexed<? extends RaftLogEntry> entry) {
    LOGGER.trace("{} - Applying {}", state.getCluster().getMember().memberId(), entry);
    if (entry.type() == QueryEntry.class) {
      return (CompletableFuture<T>) applyQuery(entry.cast());
    } else if (entry.type() == CommandEntry.class) {
      return (CompletableFuture<T>) applyCommand(entry.cast());
    } else if (entry.type() == OpenSessionEntry.class) {
      return (CompletableFuture<T>) applyOpenSession(entry.cast());
    } else if (entry.type() == KeepAliveEntry.class) {
      return (CompletableFuture<T>) applyKeepAlive(entry.cast());
    } else if (entry.type() == CloseSessionEntry.class) {
      return (CompletableFuture<T>) applyCloseSession(entry.cast());
    } else if (entry.type() == MetadataEntry.class) {
      return (CompletableFuture<T>) applyMetadata(entry.cast());
    } else if (entry.type() == InitializeEntry.class) {
      return (CompletableFuture<T>) applyInitialize(entry.cast());
    } else if (entry.type() == ConfigurationEntry.class) {
      return (CompletableFuture<T>) applyConfiguration(entry.cast());
    }
    return Futures.exceptionalFuture(new InternalException("Unknown entry type"));
  }

  /**
   * Applies an initialize entry.
   * <p>
   * Initialize entries are used only at the beginning of a new leader's term to force the commitment of entries from
   * prior terms, therefore no logic needs to take place.
   */
  private CompletableFuture<Void> applyInitialize(Indexed<InitializeEntry> entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies a configuration entry to the internal state machine.
   * <p>
   * Configuration entries are applied to internal server state when written to the log. Thus, no significant
   * logic needs to take place in the handling of configuration entries. We simply release the previous configuration
   * entry since it was overwritten by a more recent committed configuration entry.
   */
  private CompletableFuture<Void> applyConfiguration(Indexed<ConfigurationEntry> entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies a session keep alive entry to the state machine.
   * <p>
   * Keep alive entries are applied to the internal state machine to reset the timeout for a specific session.
   * If the session indicated by the KeepAliveEntry is still held in memory, we mark the session as trusted,
   * indicating that the client has committed a keep alive within the required timeout. Additionally, we check
   * all other sessions for expiration based on the timestamp provided by this KeepAliveEntry. Note that sessions
   * are never completely expired via this method. Leaders must explicitly commit an UnregisterEntry to expire
   * a session.
   * <p>
   * When a KeepAliveEntry is committed to the internal state machine, two specific fields provided in the entry
   * are used to update server-side session state. The {@code commandSequence} indicates the highest command for
   * which the session has received a successful response in the proper sequence. By applying the {@code commandSequence}
   * to the server session, we clear command output held in memory up to that point. The {@code eventVersion} indicates
   * the index up to which the client has received event messages in sequence for the session. Applying the
   * {@code eventVersion} to the server-side session results in events up to that index being removed from memory
   * as they were acknowledged by the client. It's essential that both of these fields be applied via entries committed
   * to the Raft log to ensure they're applied on all servers in sequential order.
   * <p>
   * Keep alive entries are retained in the log until the next time the client sends a keep alive entry or until the
   * client's session is expired. This ensures for sessions that have long timeouts, keep alive entries cannot be cleaned
   * from the log before they're replicated to some servers.
   */
  private CompletableFuture<Void> applyKeepAlive(Indexed<KeepAliveEntry> entry) {
    // Store the session/command/event sequence and event index instead of acquiring a reference to the entry.
    long[] sessionIds = entry.entry().sessionIds();
    long[] commandSequences = entry.entry().commandSequenceNumbers();
    long[] eventIndexes = entry.entry().eventIndexes();

    for (int i = 0; i < sessionIds.length; i++) {
      long sessionId = sessionIds[i];
      long commandSequence = commandSequences[i];
      long eventIndex = eventIndexes[i];

      RaftSessionContext session = sessionManager.getSession(sessionId);
      if (session != null) {
        session.getStateMachineContext().keepAlive(entry.index(), entry.entry().timestamp(), session, commandSequence, eventIndex);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an open session entry to the state machine.
   */
  private CompletableFuture<Long> applyOpenSession(Indexed<OpenSessionEntry> entry) {
    // Get the state machine executor or create one if it doesn't already exist.
    RaftServerServiceContext stateMachineExecutor = stateMachines.get(entry.entry().serviceName());
    if (stateMachineExecutor == null) {
      Supplier<RaftStateMachine> stateMachineSupplier = state.getStateMachineRegistry().getFactory(entry.entry().serviceType());
      if (stateMachineSupplier == null) {
        return Futures.exceptionalFuture(new UnknownStateMachineException("Unknown state machine type " + entry.entry().serviceType()));
      }

      StateMachineId stateMachineId = StateMachineId.from(entry.index());
      stateMachineExecutor = new RaftServerServiceContext(
          stateMachineId,
          ServiceName.from(entry.entry().serviceName()),
          ServiceType.from(entry.entry().serviceType()),
          stateMachineSupplier.get(),
          state,
          sessionManager,
          new ThreadPoolContext(threadPool),
          new ThreadPoolContext(threadPool));
      stateMachines.put(entry.entry().serviceName(), stateMachineExecutor);
    }

    SessionId sessionId = SessionId.from(entry.index());
    RaftSessionContext session = new RaftSessionContext(
        sessionId,
        MemberId.from(entry.entry().memberId()),
        ServiceName.from(entry.entry().serviceName()),
        ServiceType.from(entry.entry().serviceType()),
        entry.entry().readConsistency(),
        entry.entry().timeout(),
        stateMachineExecutor,
        state);
    return stateMachineExecutor.openSession(entry.index(), entry.entry().timestamp(), session);
  }

  /**
   * Applies a close session entry to the state machine.
   */
  private CompletableFuture<Void> applyCloseSession(Indexed<CloseSessionEntry> entry) {
    RaftSessionContext session = sessionManager.getSession(entry.entry().session());

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown session: " + entry.entry().session()));
    }

    // Get the state machine executor associated with the session and unregister the session.
    RaftServerServiceContext stateMachineExecutor = session.getStateMachineContext();
    return stateMachineExecutor.closeSession(entry.index(), entry.entry().timestamp(), session);
  }

  /**
   * Applies a metadata entry to the state machine.
   */
  private CompletableFuture<RaftMetadataResult> applyMetadata(Indexed<MetadataEntry> entry) {
    // If the session ID is non-zero, read the metadata for the associated state machine.
    if (entry.entry().session() > 0) {
      RaftSessionContext session = sessionManager.getSession(entry.entry().session());

      // If the session is null, return an UnknownSessionException.
      if (session == null) {
        return Futures.exceptionalFuture(new UnknownSessionException("Unknown session: " + entry.entry().session()));
      }

      Set<RaftSessionMetadata> sessions = new HashSet<>();
      for (RaftSessionContext s : sessionManager.getSessions()) {
        if (s.serviceName().equals(session.serviceName())) {
          sessions.add(new RaftSessionMetadata(s.sessionId().id(), s.serviceName().id(), s.serviceType().id()));
        }
      }
      return CompletableFuture.completedFuture(new RaftMetadataResult(sessions));
    } else {
      Set<RaftSessionMetadata> sessions = new HashSet<>();
      for (RaftSessionContext session : sessionManager.getSessions()) {
        sessions.add(new RaftSessionMetadata(session.sessionId().id(), session.serviceName().id(), session.serviceType().id()));
      }
      return CompletableFuture.completedFuture(new RaftMetadataResult(sessions));
    }
  }

  /**
   * Applies a command entry to the state machine.
   * <p>
   * Command entries result in commands being executed on the user provided {@link RaftStateMachine} and a
   * response being sent back to the client by completing the returned future. All command responses are
   * cached in the command's {@link RaftSessionContext} for fault tolerance. In the event that the same command
   * is applied to the state machine more than once, the original response will be returned.
   * <p>
   * Command entries are written with a sequence number. The sequence number is used to ensure that
   * commands are applied to the state machine in sequential order. If a command entry has a sequence
   * number that is less than the next sequence number for the session, that indicates that it is a
   * duplicate of a command that was already applied. Otherwise, commands are assumed to have been
   * received in sequential order. The reason for this assumption is because leaders always sequence
   * commands as they're written to the log, so no sequence number will be skipped.
   */
  private CompletableFuture<OperationResult> applyCommand(Indexed<CommandEntry> entry) {
    // First check to ensure that the session exists.
    RaftSessionContext session = sessionManager.getSession(entry.entry().session());

    // If the session is null, return an UnknownSessionException. Commands applied to the state machine must
    // have a session. We ensure that session register/unregister entries are not compacted from the log
    // until all associated commands have been cleaned.
    if (session == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.entry().session()));
    }

    // Execute the command using the state machine associated with the session.
    return session.getStateMachineContext()
        .executeCommand(
            entry.index(),
            entry.entry().sequenceNumber(),
            entry.entry().timestamp(),
            session,
            entry.entry().operation());
  }

  /**
   * Applies a query entry to the state machine.
   * <p>
   * Query entries are applied to the user {@link RaftStateMachine} for read-only operations.
   * Because queries are read-only, they may only be applied on a single server in the cluster,
   * and query entries do not go through the Raft log. Thus, it is critical that measures be taken
   * to ensure clients see a consistent view of the cluster event when switching servers. To do so,
   * clients provide a sequence and version number for each query. The sequence number is the order
   * in which the query was sent by the client. Sequence numbers are shared across both commands and
   * queries. The version number indicates the last index for which the client saw a command or query
   * response. In the event that the lastApplied index of this state machine does not meet the provided
   * version number, we wait for the state machine to catch up before applying the query. This ensures
   * clients see state progress monotonically even when switching servers.
   * <p>
   * Because queries may only be applied on a single server in the cluster they cannot result in the
   * publishing of session events. Events require commands to be written to the Raft log to ensure
   * fault-tolerance and consistency across the cluster.
   */
  private CompletableFuture<OperationResult> applyQuery(Indexed<QueryEntry> entry) {
    RaftSessionContext session = sessionManager.getSession(entry.entry().session());

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.entry().session()));
    }

    // Execute the query using the state machine associated with the session.
    return session.getStateMachineContext()
        .executeQuery(
            entry.index(),
            entry.entry().sequenceNumber(),
            entry.entry().timestamp(),
            session,
            entry.entry().operation());
  }

  /**
   * Compacts the log if necessary.
   */
  private void compactLog() {
    // Iterate through state machines and compute the lowest stored snapshot for all state machines.
    long snapshotIndex = state.getLogWriter().getLastIndex();
    for (RaftServerServiceContext stateMachineExecutor : stateMachines.values()) {
      Snapshot snapshot = state.getSnapshotStore().getSnapshotById(stateMachineExecutor.stateMachineId());
      if (snapshot == null) {
        return;
      } else {
        snapshotIndex = Math.min(snapshotIndex, snapshot.index());
      }
    }

    // Compact logs prior to the lowest snapshot.
    state.getLog().compact(snapshotIndex);
  }

  @Override
  public void close() {
    // Don't close the thread context here since state machines can be reused.
  }
}
