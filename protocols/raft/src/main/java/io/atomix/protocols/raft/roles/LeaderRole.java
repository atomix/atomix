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
package io.atomix.protocols.raft.roles;

import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftMemberContext;
import io.atomix.protocols.raft.impl.MetadataResult;
import io.atomix.protocols.raft.impl.OperationResult;
import io.atomix.protocols.raft.impl.RaftContext;
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
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.protocol.TransferResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.session.RaftSession;
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
import io.atomix.storage.StorageException;
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
  private static final int MAX_APPEND_ATTEMPTS = 5;

  private final ClusterEventListener clusterListener = this::handleClusterEvent;
  private final LeaderAppender appender;
  private Scheduled appendTimer;
  private long configuring;
  private boolean transferring;

  public LeaderRole(RaftContext context) {
    super(context);
    this.appender = new LeaderAppender(this);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.LEADER;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> start() {
    // Reset state for the leader.
    takeLeadership();

    // Append initial entries to the log, including an initial no-op entry and the server's configuration.
    appendInitialEntries().join();

    // Commit the initial leader entries.
    commitInitialEntries();

    // Register the cluster event listener.
    raft.getClusterService().addListener(clusterListener);

    return super.start()
        .thenRun(this::startAppendTimer)
        .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    raft.setLeader(raft.getCluster().getMember().nodeId());
    raft.getCluster().getRemoteMemberStates().forEach(m -> m.resetState(raft.getLog()));
  }

  /**
   * Appends initial entries to the log to take leadership.
   */
  private CompletableFuture<Void> appendInitialEntries() {
    final long term = raft.getTerm();

    return appendAndCompact(new InitializeEntry(term, appender.getTime()))
        .whenComplete((entry, error) -> {
          if (error == null) {
            log.trace("Appended {}", entry);
          }
        }).thenApply(index -> null);
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
    appender.appendEntries(appender.getIndex()).whenComplete((resultIndex, error) -> {
      raft.checkThread();
      if (isRunning()) {
        if (error == null) {
          raft.getStateMachine().apply(resultIndex);
          future.complete(null);
        } else {
          raft.setLeader(null);
          raft.transition(RaftServer.Role.FOLLOWER);
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
    log.trace("Starting append timer");
    appendTimer = raft.getThreadContext().schedule(Duration.ZERO, raft.getHeartbeatInterval(), this::appendMembers);
  }

  /**
   * Sends AppendEntries requests to members of the cluster that haven't heard from the leader in a while.
   */
  private void appendMembers() {
    raft.checkThread();
    if (isRunning()) {
      appender.appendEntries();
    }
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterEvent event) {
    if (event.type() == ClusterEvent.Type.NODE_DEACTIVATED) {
      log.debug("Node {} deactivated", event.subject().id());
      raft.getSessions().getSessions().stream()
          .filter(session -> session.nodeId().equals(event.subject().id()))
          .forEach(this::expireSession);
    }
  }

  /**
   * Expires the given session.
   */
  private void expireSession(RaftSession session) {
    log.debug("Expiring session: {}", session);
    appendAndCompact(new CloseSessionEntry(raft.getTerm(), System.currentTimeMillis(), session.sessionId().id(), true))
        .whenCompleteAsync((entry, error) -> {
          log.trace("Appended {}", entry);
          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning() && commitError == null) {
              raft.getStateMachine().<Long>apply(entry.index());
            }
          });
        }, raft.getThreadContext());
  }

  /**
   * Returns a boolean value indicating whether a configuration is currently being committed.
   *
   * @return Indicates whether a configuration is currently being committed.
   */
  private boolean configuring() {
    return configuring > 0;
  }

  /**
   * Returns a boolean value indicating whether the leader is still being initialized.
   *
   * @return Indicates whether the leader is still being initialized.
   */
  private boolean initializing() {
    // If the leader index is 0 or is greater than the commitIndex, do not allow configuration changes.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    return appender.getIndex() == 0 || raft.getCommitIndex() < appender.getIndex();
  }

  /**
   * Commits the given configuration.
   */
  protected CompletableFuture<Long> configure(Collection<RaftMember> members) {
    raft.checkThread();

    final long term = raft.getTerm();

    return appendAndCompact(new ConfigurationEntry(term, System.currentTimeMillis(), members))
        .thenComposeAsync(entry -> {
          log.trace("Appended {}", entry);

          // Store the index of the configuration entry in order to prevent other configurations from
          // being logged and committed concurrently. This is an important safety property of Raft.
          configuring = entry.index();
          raft.getCluster().configure(new Configuration(entry.index(), entry.entry().term(), entry.entry().timestamp(), entry.entry().members()));

          return appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning() && commitError == null) {
              raft.getStateMachine().<OperationResult>apply(entry.index());
            }
            configuring = 0;
          });
        }, raft.getThreadContext());
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(final JoinRequest request) {
    raft.checkThread();
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
    if (raft.getCluster().getMember(request.member().nodeId()) != null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withIndex(raft.getCluster().getConfiguration().index())
          .withTerm(raft.getCluster().getConfiguration().term())
          .withTime(raft.getCluster().getConfiguration().time())
          .withMembers(raft.getCluster().getMembers())
          .build()));
    }

    RaftMember member = request.member();

    // Add the joining member to the members list. If the joining member's type is ACTIVE, join the member in the
    // PROMOTABLE state to allow it to get caught up without impacting the quorum size.
    Collection<RaftMember> members = raft.getCluster().getMembers();
    members.add(new DefaultRaftMember(member.nodeId(), member.getType(), Instant.now()));

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(JoinResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(index)
            .withTerm(raft.getCluster().getConfiguration().term())
            .withTime(raft.getCluster().getConfiguration().time())
            .withMembers(members)
            .build()));
      } else {
        future.complete(logResponse(JoinResponse.builder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(final ReconfigureRequest request) {
    raft.checkThread();
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
    DefaultRaftMember existingMember = raft.getCluster().getMember(request.member().nodeId());
    if (existingMember == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION)
          .build()));
    }

    // If the configuration request index is less than the last known configuration index for
    // the leader, fail the request to ensure servers can't reconfigure an old configuration.
    if (request.index() > 0 && request.index() < raft.getCluster().getConfiguration().index()
        || request.term() != raft.getCluster().getConfiguration().term()) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.CONFIGURATION_ERROR)
          .build()));
    }

    // If the member type has not changed, complete the configuration change successfully.
    if (existingMember.getType() == request.member().getType()) {
      Configuration configuration = raft.getCluster().getConfiguration();
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withIndex(configuration.index())
          .withTerm(raft.getCluster().getConfiguration().term())
          .withTime(raft.getCluster().getConfiguration().time())
          .withMembers(configuration.members())
          .build()));
    }

    // Update the member type.
    existingMember.update(request.member().getType(), Instant.now());

    Collection<RaftMember> members = raft.getCluster().getMembers();

    CompletableFuture<ReconfigureResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(ReconfigureResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(index)
            .withTerm(raft.getCluster().getConfiguration().term())
            .withTime(raft.getCluster().getConfiguration().time())
            .withMembers(members)
            .build()));
      } else {
        future.complete(logResponse(ReconfigureResponse.builder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(final LeaveRequest request) {
    raft.checkThread();
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
    if (raft.getCluster().getMember(request.member().nodeId()) == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withMembers(raft.getCluster().getMembers())
          .build()));
    }

    RaftMember member = request.member();

    Collection<RaftMember> members = raft.getCluster().getMembers();
    members.remove(member);

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(LeaveResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(index)
            .withTerm(raft.getCluster().getConfiguration().term())
            .withTime(raft.getCluster().getConfiguration().time())
            .withMembers(members)
            .build()));
      } else {
        future.complete(logResponse(LeaveResponse.builder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<TransferResponse> onTransfer(final TransferRequest request) {
    logRequest(request);

    RaftMemberContext member = raft.getCluster().getMemberState(request.member());
    if (member == null) {
      return CompletableFuture.completedFuture(logResponse(TransferResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    transferring = true;

    CompletableFuture<TransferResponse> future = new CompletableFuture<>();
    appender.appendEntries(raft.getLogWriter().getLastIndex()).whenComplete((result, error) -> {
      if (isRunning()) {
        if (error == null) {
          log.debug("Transferring leadership to {}", request.member());
          raft.transition(RaftServer.Role.FOLLOWER);
          future.complete(logResponse(TransferResponse.builder()
              .withStatus(RaftResponse.Status.OK)
              .build()));
        } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
          future.complete(logResponse(TransferResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(((RaftException) error.getCause()).getType(), error.getMessage())
              .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(TransferResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(((RaftException) error).getType(), error.getMessage())
              .build()));
        } else {
          future.complete(logResponse(TransferResponse.builder()
              .withStatus(RaftResponse.Status.ERROR)
              .withError(RaftError.Type.PROTOCOL_ERROR, error.getMessage())
              .build()));
        }
      } else {
        future.complete(logResponse(TransferResponse.builder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(final PollRequest request) {
    logRequest(request);

    // If a member sends a PollRequest to the leader, that indicates that it likely healed from
    // a network partition and may have had its status set to UNAVAILABLE by the leader. In order
    // to ensure heartbeats are immediately stored to the member, update its status if necessary.
    RaftMemberContext member = raft.getCluster().getMemberState(request.candidate());
    if (member != null) {
      member.resetFailureCount();
    }

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(raft.getTerm())
        .withAccepted(false)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(final VoteRequest request) {
    if (updateTermAndLeader(request.term(), null)) {
      log.debug("Received greater term");
      raft.transition(RaftServer.Role.FOLLOWER);
      return super.onVote(request);
    } else {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(raft.getTerm())
          .withVoted(false)
          .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    raft.checkThread();
    if (updateTermAndLeader(request.term(), request.leader())) {
      CompletableFuture<AppendResponse> future = super.onAppend(request);
      raft.transition(RaftServer.Role.FOLLOWER);
      return future;
    } else if (request.term() < raft.getTerm()) {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
          .withStatus(RaftResponse.Status.OK)
          .withTerm(raft.getTerm())
          .withSucceeded(false)
          .withLastLogIndex(raft.getLogWriter().getLastIndex())
          .build()));
    } else {
      raft.setLeader(request.leader());
      raft.transition(RaftServer.Role.FOLLOWER);
      return super.onAppend(request);
    }
  }

  @Override
  public CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request) {
    raft.checkThread();
    logRequest(request);

    if (transferring) {
      return CompletableFuture.completedFuture(logResponse(MetadataResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    Indexed<MetadataEntry> entry = new Indexed<>(
        raft.getLastApplied(),
        new MetadataEntry(raft.getTerm(), System.currentTimeMillis(), request.session()), 0);
    raft.getStateMachine().<MetadataResult>apply(entry).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(logResponse(MetadataResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withSessions(result.sessions())
            .build()));
      } else {
        future.complete(logResponse(MetadataResponse.builder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(final CommandRequest request) {
    raft.checkThread();
    logRequest(request);

    if (transferring) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    RaftSession session = raft.getSessions().getSession(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION)
          .build()));
    }

    final long sequenceNumber = request.sequenceNumber();

    final CompletableFuture<CommandResponse> future = new CompletableFuture<>();

    // If the session's current sequence number is less then one prior to the request sequence number, reject
    // the command to force it to be resent in the correct order. Note that it's possible for the session
    // sequence number to be greater than the request sequence number. In that case, it's likely that the
    // command was submitted more than once to the cluster, and the command will be deduplicated once
    // applied to the state machine.
    if (sequenceNumber > session.nextRequestSequence()) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.COMMAND_FAILURE)
          .withLastSequence(session.getRequestSequence())
          .build()));
    }

    // If the command has already been applied to the state machine then return a cached result if possible, otherwise
    // return null.
    if (sequenceNumber <= session.getCommandSequence()) {
      OperationResult result = session.getResult(sequenceNumber);
      if (result != null) {
        completeOperation(result, CommandResponse.builder(), null, future);
      } else {
        future.complete(CommandResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(session.getLastApplied())
            .withEventIndex(0)
            .withResult(new byte[0])
            .build());
      }
    }
    // Otherwise, commit the command and update the request sequence number.
    else {
      commitCommand(request, future);
      session.setRequestSequence(sequenceNumber);
    }

    return future.thenApply(this::logResponse);
  }

  /**
   * Commits a command.
   *
   * @param request the command request
   * @param future  the command response future
   */
  private void commitCommand(CommandRequest request, CompletableFuture<CommandResponse> future) {
    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();

    appendAndCompact(new CommandEntry(term, timestamp, request.session(), request.sequenceNumber(), request.operation()))
        .whenCompleteAsync((entry, error) -> {
          if (error != null) {
            future.complete(CommandResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.COMMAND_FAILURE)
                .build());
            return;
          }

          log.trace("Appended {}", entry);

          // Replicate the command to followers.
          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              // If the command was successfully committed, apply it to the state machine.
              if (commitError == null) {
                raft.getStateMachine().<OperationResult>apply(entry.index()).whenComplete((r, e) -> {
                  completeOperation(r, CommandResponse.builder(), e, future);
                });
              } else {
                future.complete(CommandResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(RaftError.Type.PROTOCOL_ERROR)
                    .build());
              }
            } else {
              future.complete(CommandResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.COMMAND_FAILURE)
                  .build());
            }
          });
        }, raft.getThreadContext());
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(final QueryRequest request) {
    raft.checkThread();
    logRequest(request);

    // If this server has not yet applied entries up to the client's session ID, forward the
    // query to the leader. This ensures that a follower does not tell the client its session
    // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
    if (raft.getLastApplied() < request.session()) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION, "Session has not yet been created. You're seeing into the future!")
          .build()));
    }

    // Look up the client's session.
    RaftSession session = raft.getSessions().getSession(request.session());
    if (session == null) {
      log.warn("Unknown session {}", request.session());
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.UNKNOWN_SESSION)
          .build()));
    }

    final Indexed<QueryEntry> entry = new Indexed<>(
        request.index(),
        new QueryEntry(
            raft.getTerm(),
            System.currentTimeMillis(),
            request.session(),
            request.sequenceNumber(),
            request.operation()), 0);

    final CompletableFuture<QueryResponse> future;
    switch (session.readConsistency()) {
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
        future = Futures.exceptionalFuture(new IllegalStateException("Unknown consistency level: " + session.readConsistency()));
        break;
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
        .thenComposeAsync(response -> appender.appendEntries()
            .thenApply(index -> response)
            .exceptionally(error -> QueryResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.QUERY_FAILURE, error.getMessage())
                .build()), raft.getThreadContext());
  }

  @Override
  public CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request) {
    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();
    final long minTimeout = request.minTimeout();

    // If the client submitted a session timeout, use the client's timeout, otherwise use the configured
    // default server session timeout.
    final long maxTimeout;
    if (request.maxTimeout() != 0) {
      maxTimeout = request.maxTimeout();
    } else {
      maxTimeout = raft.getSessionTimeout().toMillis();
    }

    raft.checkThread();
    logRequest(request);

    CompletableFuture<OpenSessionResponse> future = new CompletableFuture<>();
    appendAndCompact(new OpenSessionEntry(term, timestamp, request.node(), request.serviceName(), request.serviceType(), request.readConsistency(), minTimeout, maxTimeout))
        .whenCompleteAsync((entry, error) -> {
          if (error != null) {
            future.complete(logResponse(OpenSessionResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.PROTOCOL_ERROR)
                .build()));
            return;
          }

          log.trace("Appended {}", entry);

          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              if (commitError == null) {
                raft.getStateMachine().<Long>apply(entry.index()).whenComplete((sessionId, sessionError) -> {
                  if (sessionError == null) {
                    future.complete(logResponse(OpenSessionResponse.builder()
                        .withStatus(RaftResponse.Status.OK)
                        .withSession(sessionId)
                        .withTimeout(maxTimeout)
                        .build()));
                  } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof RaftException) {
                    future.complete(logResponse(OpenSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(((RaftException) sessionError.getCause()).getType(), sessionError.getMessage())
                        .build()));
                  } else if (sessionError instanceof RaftException) {
                    future.complete(logResponse(OpenSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(((RaftException) sessionError).getType(), sessionError.getMessage())
                        .build()));
                  } else {
                    future.complete(logResponse(OpenSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(RaftError.Type.PROTOCOL_ERROR, sessionError.getMessage())
                        .build()));
                  }
                });
              } else {
                future.complete(logResponse(OpenSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(RaftError.Type.PROTOCOL_ERROR)
                    .build()));
              }
            } else {
              future.complete(logResponse(OpenSessionResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
                  .build()));
            }
          });
        }, raft.getThreadContext());

    return future;
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();

    raft.checkThread();
    logRequest(request);

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    appendAndCompact(new KeepAliveEntry(term, timestamp, request.sessionIds(), request.commandSequenceNumbers(), request.eventIndexes()))
        .whenCompleteAsync((entry, error) -> {
          if (error != null) {
            future.complete(logResponse(KeepAliveResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withLeader(raft.getCluster().getMember().nodeId())
                .withError(RaftError.Type.PROTOCOL_ERROR)
                .build()));
            return;
          }

          log.trace("Appended {}", entry);

          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              if (commitError == null) {
                raft.getStateMachine().<long[]>apply(entry.index()).whenCompleteAsync((sessionResult, sessionError) -> {
                  if (sessionError == null) {
                    future.complete(logResponse(KeepAliveResponse.builder()
                        .withStatus(RaftResponse.Status.OK)
                        .withLeader(raft.getCluster().getMember().nodeId())
                        .withMembers(raft.getCluster().getMembers().stream()
                            .map(RaftMember::nodeId)
                            .filter(m -> m != null)
                            .collect(Collectors.toList()))
                        .withSessionIds(sessionResult)
                        .build()));
                  } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof RaftException) {
                    future.complete(logResponse(KeepAliveResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withLeader(raft.getCluster().getMember().nodeId())
                        .withError(((RaftException) sessionError.getCause()).getType(), sessionError.getMessage())
                        .build()));
                  } else if (sessionError instanceof RaftException) {
                    future.complete(logResponse(KeepAliveResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withLeader(raft.getCluster().getMember().nodeId())
                        .withError(((RaftException) sessionError).getType(), sessionError.getMessage())
                        .build()));
                  } else {
                    future.complete(logResponse(KeepAliveResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withLeader(raft.getCluster().getMember().nodeId())
                        .withError(RaftError.Type.PROTOCOL_ERROR, sessionError.getMessage())
                        .build()));
                  }
                }, raft.getThreadContext());
              } else {
                future.complete(logResponse(KeepAliveResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withLeader(raft.getCluster().getMember().nodeId())
                    .withError(RaftError.Type.PROTOCOL_ERROR)
                    .build()));
              }
            } else {
              RaftMember leader = raft.getLeader();
              future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withLeader(leader != null ? leader.nodeId() : null)
                  .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
                  .build()));
            }
          });
        }, raft.getThreadContext());

    return future;
  }

  @Override
  public CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request) {
    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();

    raft.checkThread();
    logRequest(request);

    CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
    appendAndCompact(new CloseSessionEntry(term, timestamp, request.session(), false))
        .whenCompleteAsync((entry, error) -> {
          if (error != null) {
            future.complete(logResponse(CloseSessionResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.PROTOCOL_ERROR)
                .build()));
            return;
          }

          log.trace("Appended {}", entry);

          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              if (commitError == null) {
                raft.getStateMachine().<Long>apply(entry.index()).whenComplete((closeResult, closeError) -> {
                  if (closeError == null) {
                    future.complete(logResponse(CloseSessionResponse.builder()
                        .withStatus(RaftResponse.Status.OK)
                        .build()));
                  } else if (closeError instanceof CompletionException && closeError.getCause() instanceof RaftException) {
                    future.complete(logResponse(CloseSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(((RaftException) closeError.getCause()).getType(), closeError.getMessage())
                        .build()));
                  } else if (closeError instanceof RaftException) {
                    future.complete(logResponse(CloseSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(((RaftException) closeError).getType(), closeError.getMessage())
                        .build()));
                  } else {
                    future.complete(logResponse(CloseSessionResponse.builder()
                        .withStatus(RaftResponse.Status.ERROR)
                        .withError(RaftError.Type.PROTOCOL_ERROR, closeError.getMessage())
                        .build()));
                  }
                });
              } else {
                future.complete(logResponse(CloseSessionResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(RaftError.Type.PROTOCOL_ERROR)
                    .build()));
              }
            } else {
              future.complete(logResponse(CloseSessionResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.ILLEGAL_MEMBER_STATE)
                  .build()));
            }
          });
        }, raft.getThreadContext());

    return future;
  }

  /**
   * Appends an entry to the Raft log and compacts logs if necessary.
   *
   * @param entry the entry to append
   * @param <E>   the entry type
   * @return a completable future to be completed once the entry has been appended
   */
  private <E extends RaftLogEntry> CompletableFuture<Indexed<E>> appendAndCompact(E entry) {
    return appendAndCompact(entry, 0);
  }

  /**
   * Appends an entry to the Raft log and compacts logs if necessary.
   *
   * @param entry   the entry to append
   * @param attempt the append attempt count
   * @param <E>     the entry type
   * @return a completable future to be completed once the entry has been appended
   */
  protected <E extends RaftLogEntry> CompletableFuture<Indexed<E>> appendAndCompact(E entry, int attempt) {
    if (attempt == MAX_APPEND_ATTEMPTS) {
      return Futures.exceptionalFuture(new StorageException.OutOfDiskSpace("Not enough space to append entry"));
    } else {
      try {
        return CompletableFuture.completedFuture(raft.getLogWriter().append(entry));
      } catch (StorageException.OutOfDiskSpace e) {
        return raft.getLogCompactor().compact().thenCompose(v -> appendAndCompact(entry, attempt + 1));
      }
    }
  }

  /**
   * Cancels the append timer.
   */
  private void cancelAppendTimer() {
    if (appendTimer != null) {
      log.trace("Cancelling append timer");
      appendTimer.cancel();
    }
  }

  /**
   * Ensures the local server is not the leader.
   */
  private void stepDown() {
    if (raft.getLeader() != null && raft.getLeader().equals(raft.getCluster().getMember())) {
      raft.setLeader(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    raft.getClusterService().removeListener(clusterListener);
    return super.stop()
        .thenRun(appender::close)
        .thenRun(this::cancelAppendTimer)
        .thenRun(this::stepDown);
  }

}
