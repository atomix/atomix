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
package io.atomix.protocols.log.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.ManagedMemberGroupService;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.protocols.log.DistributedLogServer;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.protocols.log.roles.FollowerRole;
import io.atomix.protocols.log.roles.LeaderRole;
import io.atomix.protocols.log.roles.LogServerRole;
import io.atomix.protocols.log.roles.NoneRole;
import io.atomix.storage.journal.JournalReader;
import io.atomix.storage.journal.JournalSegment;
import io.atomix.storage.journal.JournalWriter;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLogger;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.protocols.log.DistributedLogServer.Role;

/**
 * Primary-backup server context.
 */
public class DistributedLogServerContext implements Managed<Void> {
  private final Logger log;
  private final String serverName;
  private final MemberId memberId;
  private final ClusterMembershipService clusterMembershipService;
  private final ManagedMemberGroupService memberGroupService;
  private final LogServerProtocol protocol;
  private final int replicationFactor;
  private final Replication replicationStrategy;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final boolean closeOnStop;
  private final PrimaryElection primaryElection;
  private MemberId leader;
  private List<MemberId> followers;
  private LogServerRole role;
  private long currentTerm;
  private long commitIndex;
  private final SegmentedJournal<LogEntry> journal;
  private final JournalWriter<LogEntry> writer;
  private final JournalReader<LogEntry> reader;
  private final long maxLogSize;
  private final Duration maxLogAge;
  private Scheduled compactTimer;
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeRole(event.term());
  private final AtomicBoolean started = new AtomicBoolean();

  public DistributedLogServerContext(
      String serverName,
      ClusterMembershipService clusterMembershipService,
      ManagedMemberGroupService memberGroupService,
      LogServerProtocol protocol,
      PrimaryElection primaryElection,
      int replicationFactor,
      Replication replicationStrategy,
      SegmentedJournal<LogEntry> journal,
      long maxLogSize,
      Duration maxLogAge,
      ThreadContextFactory threadContextFactory,
      boolean closeOnStop) {
    this.serverName = serverName;
    this.memberId = clusterMembershipService.getLocalMember().id();
    this.clusterMembershipService = clusterMembershipService;
    this.memberGroupService = memberGroupService;
    this.protocol = protocol;
    this.replicationFactor = replicationFactor;
    this.replicationStrategy = replicationStrategy;
    this.threadContextFactory = threadContextFactory;
    this.threadContext = threadContextFactory.createContext();
    this.closeOnStop = closeOnStop;
    this.journal = journal;
    this.writer = journal.writer();
    this.reader = journal.openReader(1);
    this.maxLogSize = maxLogSize;
    this.maxLogAge = maxLogAge;
    this.primaryElection = primaryElection;
    this.log = new ContextualLogger(LoggerFactory.getLogger(getClass()),
        LoggerContext.builder(getClass())
            .addValue(serverName)
            .build());
  }

  /**
   * Returns the server name.
   *
   * @return the server name
   */
  public String serverName() {
    return serverName;
  }

  /**
   * Returns the server member ID.
   *
   * @return the server member ID
   */
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the log server protocol.
   *
   * @return the log server protocol
   */
  public LogServerProtocol protocol() {
    return protocol;
  }

  /**
   * Returns the log sever journal.
   *
   * @return the log sever journal
   */
  public SegmentedJournal<LogEntry> journal() {
    return journal;
  }

  /**
   * Returns the log server journal writer.
   *
   * @return the log server journal writer
   */
  public JournalWriter<LogEntry> writer() {
    return writer;
  }

  /**
   * Returns the log server journal reader.
   *
   * @return the log server journal reader
   */
  public JournalReader<LogEntry> reader() {
    return reader;
  }

  /**
   * Returns the replication factor.
   *
   * @return the replication factor
   */
  public int replicationFactor() {
    return replicationFactor;
  }

  /**
   * Returns the replication strategy.
   *
   * @return the replication strategy
   */
  public Replication replicationStrategy() {
    return replicationStrategy;
  }

  /**
   * Returns the server thread context.
   *
   * @return the server thread context
   */
  public ThreadContext threadContext() {
    return threadContext;
  }

  /**
   * Returns the current server role.
   *
   * @return the current server role
   */
  public DistributedLogServer.Role getRole() {
    return Objects.equals(Futures.get(primaryElection.getTerm()).primary().memberId(), clusterMembershipService.getLocalMember().id())
        ? DistributedLogServer.Role.LEADER
        : DistributedLogServer.Role.FOLLOWER;
  }

  /**
   * Returns the leader node.
   *
   * @return the leader node
   */
  public MemberId leader() {
    return leader;
  }

  /**
   * Returns a list of follower nodes.
   *
   * @return a list of follower nodes
   */
  public List<MemberId> followers() {
    return followers;
  }

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  public long currentTerm() {
    return currentTerm;
  }

  /**
   * Resets the current term to the given term.
   *
   * @param term    the term to which to reset the current term
   * @param leader the primary for the given term
   */
  public void resetTerm(long term, MemberId leader) {
    this.currentTerm = term;
    this.leader = leader;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex the commit index
   * @return the updated commit index
   */
  public long setCommitIndex(long commitIndex) {
    this.commitIndex = Math.max(this.commitIndex, commitIndex);
    writer.commit(this.commitIndex);
    return this.commitIndex;
  }

  /**
   * Returns the current commit index.
   *
   * @return the current commit index
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Compacts logs if necessary.
   */
  public void compact() {
    compactBySize();
    compactByAge();
  }

  /**
   * Compacts the log by size.
   */
  private void compactBySize() {
    if (maxLogSize > 0 && journal.size() > maxLogSize) {
      JournalSegment<LogEntry> compactSegment = null;
      Long compactIndex = null;
      for (JournalSegment<LogEntry> segment : journal.segments()) {
        Collection<JournalSegment<LogEntry>> remainingSegments = journal.segments(segment.lastIndex() + 1);
        long remainingSize = remainingSegments.stream().mapToLong(JournalSegment::size).sum();
        if (remainingSize > maxLogSize) {
          log.debug("Found outsize journal segment {}", segment.file().file());
          compactSegment = segment;
        } else if (compactSegment != null) {
          compactIndex = segment.index();
          break;
        }
      }

      if (compactIndex != null) {
        log.info("Compacting journal by size up to {}", compactIndex);
        journal.compact(compactIndex);
      }
    }
  }

  /**
   * Compacts the log by age.
   */
  private void compactByAge() {
    if (maxLogAge != null) {
      long currentTime = System.currentTimeMillis();
      JournalSegment<LogEntry> compactSegment = null;
      Long compactIndex = null;
      for (JournalSegment<LogEntry> segment : journal.segments()) {
        if (currentTime - segment.descriptor().updated() > maxLogAge.toMillis()) {
          log.debug("Found expired journal segment {}", segment.file().file());
          compactSegment = segment;
        } else if (compactSegment != null) {
          compactIndex = segment.index();
          break;
        }
      }

      if (compactIndex != null) {
        log.info("Compacting journal by age up to {}", compactIndex);
        journal.compact(compactIndex);
      }
    }
  }

  @Override
  public CompletableFuture<Void> start() {
    registerListeners();
    compactTimer = threadContext.schedule(Duration.ofSeconds(30), this::compact);
    return memberGroupService.start().thenComposeAsync(v -> {
      MemberGroup group = memberGroupService.getMemberGroup(clusterMembershipService.getLocalMember());
      primaryElection.addListener(primaryElectionListener);
      if (group != null) {
        return primaryElection.enter(new GroupMember(clusterMembershipService.getLocalMember().id(), group.id()))
            .thenApply(term -> {
              changeRole(term);
              return null;
            });
      }
      return CompletableFuture.completedFuture(null);
    }, threadContext).thenApply(v -> {
      started.set(true);
      return null;
    });
  }

  /**
   * Changes the roles.
   */
  private void changeRole(PrimaryTerm term) {
    threadContext.execute(() -> {
      if (term.term() >= currentTerm) {
        log.debug("{} - Term changed: {}", memberId, term);
        currentTerm = term.term();
        leader = term.primary() != null ? term.primary().memberId() : null;
        followers = term.backups(replicationFactor - 1)
            .stream()
            .map(GroupMember::memberId)
            .collect(Collectors.toList());

        if (Objects.equals(leader, clusterMembershipService.getLocalMember().id())) {
          if (this.role == null) {
            this.role = new LeaderRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.LEADER);
          } else if (this.role.role() != Role.LEADER) {
            this.role.close();
            this.role = new LeaderRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.LEADER);
          }
        } else if (followers.contains(clusterMembershipService.getLocalMember().id())) {
          if (this.role == null) {
            this.role = new FollowerRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.FOLLOWER);
          } else if (this.role.role() != Role.FOLLOWER) {
            this.role.close();
            this.role = new FollowerRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.FOLLOWER);
          }
        } else {
          if (this.role == null) {
            this.role = new NoneRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.NONE);
          } else if (this.role.role() != Role.NONE) {
            this.role.close();
            this.role = new NoneRole(this);
            log.debug("{} transitioning to {}", clusterMembershipService.getLocalMember().id(), Role.NONE);
          }
        }
      }
    });
  }

  /**
   * Handles an append request.
   */
  private CompletableFuture<AppendResponse> append(AppendRequest request) {
    return runOnContext(() -> role.append(request));
  }

  /**
   * Handles a backup request.
   */
  private CompletableFuture<BackupResponse> backup(BackupRequest request) {
    return runOnContext(() -> role.backup(request));
  }

  /**
   * Handles a read request.
   */
  private CompletableFuture<ConsumeResponse> consume(ConsumeRequest request) {
    return runOnContext(() -> role.consume(request));
  }

  /**
   * Handles a reset request.
   */
  private void reset(ResetRequest request) {
    role.reset(request);
  }

  private <R extends LogResponse> CompletableFuture<R> runOnContext(Supplier<CompletableFuture<R>> function) {
    CompletableFuture<R> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      function.get().whenComplete((response, error) -> {
        if (error == null) {
          future.complete(response);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Registers message listeners.
   */
  private void registerListeners() {
    protocol.registerAppendHandler(this::append);
    protocol.registerBackupHandler(this::backup);
    protocol.registerConsumeHandler(this::consume);
    protocol.registerResetConsumer(this::reset, threadContext);
  }

  /**
   * Unregisters message listeners.
   */
  private void unregisterListeners() {
    protocol.unregisterAppendHandler();
    protocol.unregisterBackupHandler();
    protocol.unregisterConsumeHandler();
    protocol.unregisterResetConsumer();
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    unregisterListeners();
    primaryElection.removeListener(primaryElectionListener);
    if (compactTimer != null) {
      compactTimer.cancel();
    }
    journal.close();
    started.set(false);
    return memberGroupService.stop().exceptionally(throwable -> {
      log.error("Failed stopping member group service", throwable);
      return null;
    }).thenRunAsync(() -> {
      if (closeOnStop) {
        threadContextFactory.close();
      }
    });
  }
}
