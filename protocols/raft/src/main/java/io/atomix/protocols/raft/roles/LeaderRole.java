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
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.error.RaftException;
import io.atomix.protocols.raft.impl.RaftMetadataResult;
import io.atomix.protocols.raft.impl.RaftOperationResult;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Leader state.
 */
public final class LeaderRole extends ActiveRole {
  private final LeaderAppender appender;
  private Scheduled appendTimer;
  private long configuring;

  public LeaderRole(RaftServerContext context) {
    super(context);
    this.appender = new LeaderAppender(this);
  }

  @Override
  public RaftServer.Role type() {
    return RaftServer.Role.LEADER;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> open() {
    // Reset state for the leader.
    takeLeadership();

    // Append initial entries to the log, including an initial no-op entry and the server's configuration.
    appendInitialEntries();

    // Commit the initial leader entries.
    commitInitialEntries();

    return super.open()
        .thenRun(this::startAppendTimer)
        .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getCluster().getMember().getMemberId());
    context.getClusterState().getRemoteMemberStates().forEach(m -> m.resetState(context.getLog()));
  }

  /**
   * Appends initial entries to the log to take leadership.
   */
  private void appendInitialEntries() {
    final long term = context.getTerm();

    final RaftLogWriter writer = context.getLogWriter();
    writer.lock().lock();
    try {
      Indexed<RaftLogEntry> indexed = writer.append(new InitializeEntry(term, appender.time()));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), indexed.index());
    } finally {
      writer.lock().unlock();
    }

    // Append a configuration entry to propagate the leader's cluster configuration.
    configure(context.getCluster().getMembers());
  }

  /**
   * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
   */
  private CompletableFuture<Void> commitInitialEntries() {
    // The Raft protocol dictates that leaders cannot commit entries from previous terms until
    // at least one entry from their current term has been stored on a majority of servers. Thus,
    // we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
    // that the commitIndex is not increased until the no-op entry (appender.index()) is committed.
    CompletableFuture<Void> future = new CompletableFuture<>();
    appender.appendEntries(appender.index()).whenComplete((resultIndex, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          context.getStateMachine().apply(resultIndex);
          future.complete(null);
        } else {
          context.setLeader(null);
          context.transition(RaftServer.Role.FOLLOWER);
        }
      }
    });
    return future;
  }

  /**
   * Starts sending AppendEntries requests to all cluster members.
   */
  private void startAppendTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.trace("{} - Starting append timer", context.getCluster().getMember().getMemberId());
    appendTimer = context.getThreadContext().schedule(Duration.ZERO, context.getHeartbeatInterval(), this::appendMembers);
  }

  /**
   * Sends AppendEntries requests to members of the cluster that haven't heard from the leader in a while.
   */
  private void appendMembers() {
    context.checkThread();
    if (isOpen()) {
      appender.appendEntries();
    }
  }

  /**
   * Returns a boolean value indicating whether a configuration is currently being committed.
   *
   * @return Indicates whether a configuration is currently being committed.
   */
  boolean configuring() {
    return configuring > 0;
  }

  /**
   * Returns a boolean value indicating whether the leader is still being initialized.
   *
   * @return Indicates whether the leader is still being initialized.
   */
  boolean initializing() {
    // If the leader index is 0 or is greater than the commitIndex, do not allow configuration changes.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    return appender.index() == 0 || context.getCommitIndex() < appender.index();
  }

  /**
   * Commits the given configuration.
   */
  protected CompletableFuture<Long> configure(Collection<RaftMember> members) {
    context.checkThread();

    final long term = context.getTerm();

    final RaftLogWriter writer = context.getLogWriter();
    final Indexed<ConfigurationEntry> entry;
    writer.lock().lock();
    try {
      entry = writer.append(new ConfigurationEntry(term, System.currentTimeMillis(), members));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), entry);
    } finally {
      writer.lock().unlock();
    }

    // Store the index of the configuration entry in order to prevent other configurations from
    // being logged and committed concurrently. This is an important safety property of Raft.
    configuring = entry.index();
    context.getClusterState().configure(new Configuration(entry.index(), entry.entry().term(), entry.entry().timestamp(), entry.entry().members()));

    return appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        // Reset the configuration index to allow new configuration changes to be committed.
        configuring = 0;
      }
    });
  }

  @Override
  public CompletableFuture<JoinResponse> join(final JoinRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .build()));
    }

    // If the member is already a known member of the cluster, complete the join successfully.
    if (context.getCluster().getMember(request.member().getMemberId()) != null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withIndex(context.getClusterState().getConfiguration().index())
          .withTerm(context.getClusterState().getConfiguration().term())
          .withTime(context.getClusterState().getConfiguration().time())
          .withMembers(context.getCluster().getMembers())
          .build()));
    }

    RaftMember member = request.member();

    // Add the joining member to the members list. If the joining member's type is ACTIVE, join the member in the
    // PROMOTABLE state to allow it to get caught up without impacting the quorum size.
    Collection<RaftMember> members = context.getCluster().getMembers();
    members.add(new DefaultRaftMember(member.getMemberId(), member.getType(), member.getStatus(), Instant.now()));

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(JoinResponse.builder()
              .withStatus(RaftResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(JoinResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(final ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the promote requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .build()));
    }

    // If the member is not a known member of the cluster, fail the promotion.
    DefaultRaftMember existingMember = context.getClusterState().getMember(request.member().getMemberId());
    if (existingMember == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    // If the configuration request index is less than the last known configuration index for
    // the leader, fail the request to ensure servers can't reconfigure an old configuration.
    if (request.index() > 0 && request.index() < context.getClusterState().getConfiguration().index() || request.term() != context.getClusterState().getConfiguration().term()
        && (existingMember.getType() != request.member().getType() || existingMember.getStatus() != request.member().getStatus())) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.CONFIGURATION_ERROR)
          .build()));
    }

    // Update the member type.
    existingMember.update(request.member().getType(), Instant.now());

    Collection<RaftMember> members = context.getCluster().getMembers();

    CompletableFuture<ReconfigureResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(ReconfigureResponse.builder()
              .withStatus(RaftResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(ReconfigureResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(final LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .build()));
    }

    // If the leaving member is not a known member of the cluster, complete the leave successfully.
    if (context.getCluster().getMember(request.member().getMemberId()) == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withMembers(context.getCluster().getMembers())
          .build()));
    }

    RaftMember member = request.member();

    Collection<RaftMember> members = context.getCluster().getMembers();
    members.remove(member);

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(LeaveResponse.builder()
              .withStatus(RaftResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(LeaveResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<PollResponse> poll(final PollRequest request) {
    logRequest(request);

    // If a member sends a PollRequest to the leader, that indicates that it likely healed from
    // a network partition and may have had its status set to UNAVAILABLE by the leader. In order
    // to ensure heartbeats are immediately stored to the member, update its status if necessary.
    RaftMemberContext member = context.getClusterState().getMemberState(request.candidate());
    if (member != null) {
      member.resetFailureCount();
      if (member.getMember().getStatus() == RaftMember.Status.UNAVAILABLE) {
        member.getMember().update(RaftMember.Status.AVAILABLE, Instant.now());
        configure(context.getCluster().getMembers());
      }
    }

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(final VoteRequest request) {
    if (updateTermAndLeader(request.term(), null)) {
      LOGGER.debug("{} - Received greater term", context.getCluster().getMember().getMemberId());
      context.transition(RaftServer.Role.FOLLOWER);
      return super.vote(request);
    } else {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    if (updateTermAndLeader(request.term(), request.leader())) {
      CompletableFuture<AppendResponse> future = super.append(request);
      context.transition(RaftServer.Role.FOLLOWER);
      return future;
    } else if (request.term() < context.getTerm()) {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(context.getTerm())
          .withSucceeded(false)
          .withLogIndex(context.getLogWriter().lastIndex())
          .build()));
    } else {
      context.setLeader(request.leader()).transition(RaftServer.Role.FOLLOWER);
      return super.append(request);
    }
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    context.checkThread();
    logRequest(request);

    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    Indexed<MetadataEntry> entry = new Indexed<>(
        context.getStateMachine().getLastApplied(),
        new MetadataEntry(context.getTerm(), System.currentTimeMillis(), request.session()), 0);
    context.getStateMachine().<RaftMetadataResult>apply(entry).whenComplete((result, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(MetadataResponse.builder()
              .withStatus(RaftResponse.Status.OK)
              .withSessions(result.getSessions())
              .build()));
        } else {
          future.complete(logResponse(MetadataResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<CommandResponse> command(final CommandRequest request) {
    context.checkThread();
    logRequest(request);

    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    RaftSessionContext session = context.getStateMachine().getSessions().getSession(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    // If the command is LINEARIZABLE and the session's current sequence number is less then one prior to the request
    // sequence number, queue this request for handling later. We want to handle command requests in the order in which
    // they were sent by the client. Note that it's possible for the session sequence number to be greater than the request
    // sequence number. In that case, it's likely that the command was submitted more than once to the
    // cluster, and the command will be deduplicated once applied to the state machine.
    if (!session.setRequestSequence(request.sequence())) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.COMMAND_ERROR)
          .withLastSequence(session.getRequestSequence())
          .build()));
    }

    final CompletableFuture<CommandResponse> future = new CompletableFuture<>();

    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();

    final Indexed<CommandEntry> entry;

    final RaftLogWriter writer = context.getLogWriter();
    writer.lock().lock();
    try {
      entry = writer.append(new CommandEntry(term, timestamp, request.session(), request.sequence(), request.bytes()));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), entry);
    } finally {
      writer.lock().unlock();
    }

    // Replicate the command to followers.
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        // If the command was successfully committed, apply it to the state machine.
        if (commitError == null) {
          context.getStateMachine().<RaftOperationResult>apply(entry.index()).whenComplete((result, error) -> {
            if (isOpen()) {
              completeOperation(result, CommandResponse.builder(), error, future);
            }
          });
        } else {
          future.complete(CommandResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build());
        }
      }
    });
    return future.thenApply(this::logResponse);
  }

  @Override
  public CompletableFuture<QueryResponse> query(final QueryRequest request) {
    final long timestamp = System.currentTimeMillis();

    context.checkThread();
    logRequest(request);

    final Indexed<QueryEntry> entry = new Indexed<>(
        request.index(),
        new QueryEntry(
            context.getTerm(),
            System.currentTimeMillis(),
            request.session(),
            request.sequence(),
            request.bytes()), 0);

    final CompletableFuture<QueryResponse> future;
    switch (request.consistency()) {
      case SEQUENTIAL:
        future = queryLocal(entry);
        break;
      case LINEARIZABLE_LEASE:
        future = queryBoundedLinearizable(entry);
        break;
      case LINEARIZABLE:
        future = queryLinearizable(entry);
        break;
      default:
        future = Futures.exceptionalFuture(new IllegalStateException("Unknown consistency level: " + request.consistency()));
    }
    return future.thenApply(this::logResponse);
  }

  /**
   * Executes a bounded linearizable query.
   * <p>
   * Bounded linearizable queries succeed as long as this server remains the leader. This is possible
   * since the leader will step down in the event it fails to contact a majority of the cluster.
   */
  private CompletableFuture<QueryResponse> queryBoundedLinearizable(Indexed<QueryEntry> entry) {
    return applyQuery(entry);
  }

  /**
   * Executes a linearizable query.
   * <p>
   * Linearizable queries are first sequenced with commands and then applied to the state machine. Once
   * applied, we verify the node's leadership prior to responding successfully to the query.
   */
  private CompletableFuture<QueryResponse> queryLinearizable(Indexed<QueryEntry> entry) {
    return applyQuery(entry)
        .thenCompose(response -> appender.appendEntries()
            .thenApply(index -> response)
            .exceptionally(error -> QueryResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.QUERY_ERROR)
                .build()));
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();

    // If the client submitted a session timeout, use the client's timeout, otherwise use the configured
    // default server session timeout.
    final long timeout;
    if (request.timeout() != 0) {
      timeout = request.timeout();
    } else {
      timeout = context.getSessionTimeout().toMillis();
    }

    context.checkThread();
    logRequest(request);

    final Indexed<OpenSessionEntry> entry;
    final RaftLogWriter writer = context.getLogWriter();
    writer.lock().lock();
    try {
      entry = writer.append(new OpenSessionEntry(term, timestamp, request.member(), request.name(), request.stateMachine(), timeout));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), entry);
    } finally {
      writer.lock().unlock();
    }

    CompletableFuture<OpenSessionResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().<Long>apply(entry.index()).whenComplete((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(OpenSessionResponse.builder()
                    .withStatus(RaftResponse.Status.OK)
                    .withSession(sessionId)
                    .withTimeout(timeout)
                    .build()));
              } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof RaftException) {
                future.complete(logResponse(OpenSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(((RaftException) sessionError.getCause()).getType())
                    .build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(OpenSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(((RaftException) sessionError).getType())
                    .build()));
              } else {
                future.complete(logResponse(OpenSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(RaftError.Type.INTERNAL_ERROR)
                    .build()));
              }
            }
          });
        } else {
          future.complete(logResponse(OpenSessionResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();

    context.checkThread();
    logRequest(request);

    final Indexed<KeepAliveEntry> entry;
    final RaftLogWriter writer = context.getLogWriter();
    writer.lock().lock();
    try {
      entry = writer.append(new KeepAliveEntry(term, timestamp, request.sessionIds(), request.commandSequences(), request.eventIndexes()));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), entry);
    } finally {
      writer.lock().unlock();
    }

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().apply(entry.index()).whenComplete((sessionResult, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(KeepAliveResponse.builder()
                    .withStatus(RaftResponse.Status.OK)
                    .withLeader(context.getCluster().getMember().getMemberId())
                    .withMembers(context.getCluster().getMembers().stream()
                        .map(RaftMember::getMemberId)
                        .filter(m -> m != null)
                        .collect(Collectors.toList())).build()));
              } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof RaftException) {
                future.complete(logResponse(KeepAliveResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withLeader(context.getCluster().getMember().getMemberId())
                    .withError(((RaftException) sessionError.getCause()).getType())
                    .build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(KeepAliveResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withLeader(context.getCluster().getMember().getMemberId())
                    .withError(((RaftException) sessionError).getType())
                    .build()));
              } else {
                future.complete(logResponse(KeepAliveResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withLeader(context.getCluster().getMember().getMemberId())
                    .withError(RaftError.Type.INTERNAL_ERROR)
                    .build()));
              }
            }
          });
        } else {
          future.complete(logResponse(KeepAliveResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withLeader(context.getCluster().getMember().getMemberId())
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();

    context.checkThread();
    logRequest(request);

    final Indexed<CloseSessionEntry> entry;
    final RaftLogWriter writer = context.getLogWriter();
    writer.lock().lock();
    try {
      entry = writer.append(new CloseSessionEntry(term, timestamp, request.session()));
      LOGGER.debug("{} - Appended {}", context.getCluster().getMember().getMemberId(), entry);
    } finally {
      writer.lock().unlock();
    }

    CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().<Long>apply(entry.index()).whenComplete((closeResult, closeError) -> {
            if (isOpen()) {
              if (closeError == null) {
                future.complete(logResponse(CloseSessionResponse.builder()
                    .withStatus(RaftResponse.Status.OK)
                    .build()));
              } else if (closeError instanceof CompletionException && closeError.getCause() instanceof RaftException) {
                future.complete(logResponse(CloseSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(((RaftException) closeError.getCause()).getType())
                    .build()));
              } else if (closeError instanceof RaftException) {
                future.complete(logResponse(CloseSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(((RaftException) closeError).getType())
                    .build()));
              } else {
                future.complete(logResponse(CloseSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(RaftError.Type.INTERNAL_ERROR)
                    .build()));
              }
            }
          });
        } else {
          future.complete(logResponse(CloseSessionResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  /**
   * Cancels the append timer.
   */
  private void cancelAppendTimer() {
    if (appendTimer != null) {
      LOGGER.trace("{} - Cancelling append timer", context.getCluster().getMember().getMemberId());
      appendTimer.cancel();
    }
  }

  /**
   * Ensures the local server is not the leader.
   */
  private void stepDown() {
    if (context.getLeader() != null && context.getLeader().equals(context.getCluster().getMember())) {
      context.setLeader(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close()
        .thenRun(appender::close)
        .thenRun(this::cancelAppendTimer)
        .thenRun(this::stepDown);
  }

}
