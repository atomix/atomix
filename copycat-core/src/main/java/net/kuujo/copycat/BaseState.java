/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.util.Map;
import java.util.Set;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Base replica state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseState implements State {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  protected CopyCatContext context;

  @Override
  public void init(CopyCatContext context) {
    this.context = context;
    context.cluster.localMember().protocol().server().pingCallback(new AsyncCallback<PingRequest>() {
      @Override
      public void complete(PingRequest request) {
        try {
          handlePing(request);
        } catch (Exception e) {
          request.respond(e);
        }
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    context.cluster.localMember().protocol().server().syncCallback(new AsyncCallback<SyncRequest>() {
      @Override
      public void complete(SyncRequest request) {
        try {
          handleSync(request);
        } catch (Exception e) {
          request.respond(e);
        }
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    context.cluster.localMember().protocol().server().installCallback(new AsyncCallback<InstallRequest>() {
      @Override
      public void complete(InstallRequest request) {
        try {
          handleInstall(request);
        } catch (Exception e) {
          request.respond(e);
        }
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    context.cluster.localMember().protocol().server().pollCallback(new AsyncCallback<PollRequest>() {
      @Override
      public void complete(PollRequest request) {
        try {
          handlePoll(request);
        } catch (Exception e) {
          request.respond(e);
        }
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    context.cluster.localMember().protocol().server().submitCallback(new AsyncCallback<SubmitRequest>() {
      @Override
      public void complete(SubmitRequest request) {
        try {
          handleSubmit(request);
        } catch (Exception e) {
          request.respond(e);
        }
      }
      @Override
      public void fail(Throwable t) {
      }
    });
  }

  /**
   * Handles a ping request.
   */
  protected void handlePing(PingRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if ((request.term() > context.getCurrentTerm()) || (request.term() == context.getCurrentTerm() && context.getCurrentLeader() == null)) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(request.leader());
      context.transition(Follower.class);
    }
    request.respond(context.getCurrentTerm());
  }

  /**
   * Handles a sync request.
   */
  protected void handleSync(SyncRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    boolean transition = false;
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(request.leader());
      transition = true;
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getCurrentTerm()) {
      request.respond(context.getCurrentTerm(), false);
    } else if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      checkPreviousEntry(request);
    } else {
      appendEntries(request);
    }

    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      context.transition(Follower.class);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private void checkPreviousEntry(SyncRequest request) {
    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    Entry entry = context.log.getEntry(request.prevLogIndex());
    if (entry == null || entry.term() != request.prevLogTerm()) {
      request.respond(context.getCurrentTerm(), false);
    } else {
      appendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  private void appendEntries(SyncRequest request) {
    for (int i = 0; i < request.entries().size(); i++) {
      // Get the entry at the current index from the log.
      Entry entry = context.log.getEntry(i + request.prevLogIndex() + 1);
      // If the log does not contain an entry at this index then this
      // indicates no conflict, append the new entry.
      if (entry == null) {
        context.log.appendEntry(request.entries().get(i));
      } else if (entry.term() != request.entries().get(i).term()) {
        // If the local log's equivalent entry's term does not match the
        // synced entry's term then that indicates that it came from a
        // different leader. The log must be purged of this entry and all
        // entries following it.
        context.log.removeAfter(request.prevLogIndex());
        context.log.appendEntry(request.entries().get(i));
      }
    }
    applyCommits(request);
  }

  /**
   * Applies commits to the local state machine.
   */
  @SuppressWarnings("unchecked")
  private void applyCommits(SyncRequest request) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (request.commit() > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = context.log.lastIndex();
      context.setCommitIndex(Math.min(Math.max(request.commit(), context.getCommitIndex()), lastIndex));

      // If the updated commit index indicates that commits remain to be
      // applied to the state machine, iterate entries and apply them.
      if (context.getCommitIndex() > context.getLastApplied()) {
        // Starting after the last applied entry, iterate through new entries
        // and apply them to the state machine up to the commit index.
        for (long i = context.getLastApplied() + 1; i <= Math.min(context.getCommitIndex(), lastIndex); i++) {
          // Load the log entry to be applied to the state machine.
          Entry entry = context.log.getEntry(i);

          // If no entry exists in this position then just break the loop. Even if
          // there are entries after this for some reason, we can't apply them since
          // entries must be applied in log order.
          if (entry == null) {
            break;
          }

          // If the entry was successfully loaded then apply it to the state machine.
          if (entry instanceof CommandEntry) {
            CommandEntry command = (CommandEntry) entry;
            try {
              context.stateMachine.applyCommand(command.command(), command.args());
            } catch (Exception e) {
            }
          }
          // Configuration entries are applied to the immutable context configuration.
          else if (entry instanceof ConfigurationEntry) {
            Set<String> members = ((ConfigurationEntry) entry).members();
            members.remove(context.cluster.config().getLocalMember());
            context.cluster.config().setRemoteMembers(members);
          }
          // It's possible that an entry being committed is a snapshot entry. In
          // cases where a leader has replicated a snapshot to this node's log,
          // once the snapshot has been committed we can clear all entries before
          // the snapshot entry and apply the snapshot to the state machine.
          else if (entry instanceof SnapshotEntry) {
            Map<String, Object> snapshot = serializer.readValue(((SnapshotEntry) entry).data(), Map.class);
            context.stateMachine.installSnapshot(snapshot);
            context.log.removeBefore(i);
          }
          context.setLastApplied(i);
        }

        // Once entries have been applied check whether we need to compact the log.
        // If the log has now grown past the maximum local log size, create a
        // snapshot of the current state machine state and replace the applied
        // entries with a single snapshot entry. The snapshot is stored as a log
        // entry in order to make replication simpler if the node becomes a leader.
        if (context.log.size() > context.config().getMaxLogSize()) {
          Map<String, Object> snapshot = context.stateMachine.createSnapshot();
          byte[] bytes = serializer.writeValue(snapshot);
          SnapshotEntry entry = new SnapshotEntry(context.getCurrentTerm(), context.cluster.config().getMembers(), bytes, true);
          context.log.setEntry(context.getLastApplied(), entry);
          context.log.removeBefore(context.getLastApplied());
        }
      }
    }
    request.respond(context.getCurrentTerm(), true);
  }

  /**
   * Handles an install request.
   */
  protected void handleInstall(InstallRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    boolean transition = false;
    if ((request.term() > context.getCurrentTerm()) || (request.term() == context.getCurrentTerm() && context.getCurrentLeader() == null)) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(request.leader());
      transition = true;
    }

    // Load the entry at the given position in the log.
    Entry entry = context.log.getEntry(request.snapshotIndex());

    // If the entry doesn't exist or if the entry isn't a snapshot entry or the
    // existing snapshot entry's term doesn't match the given snapshot term then
    // add a new snapshot entry to the log.
    SnapshotEntry snapshot;
    if (entry == null || !(entry instanceof SnapshotEntry) || ((SnapshotEntry) entry).term() != request.snapshotTerm()) {
      snapshot = new SnapshotEntry(request.snapshotTerm(), request.cluster(), request.data(), request.complete());
      context.log.setEntry(request.snapshotIndex(), snapshot);
    } else {
      // If the entry already existed then we need to append the request to the
      // existing entry. Snapshot entries are sent in chunks.
      int oldLength = ((SnapshotEntry) entry).data().length;
      int newLength = request.data().length;
      byte[] combined = new byte[oldLength + newLength];
      System.arraycopy(((SnapshotEntry) entry).data(), 0, combined, 0, oldLength);
      System.arraycopy(request.data(), 0, combined, oldLength, newLength);
      snapshot = new SnapshotEntry(request.snapshotTerm(), request.cluster(), combined, request.complete());
    }

    // Respond to the install request.
    request.respond(context.getCurrentTerm(), true);

    // Note that we don't clear the log or apply the snapshot here. We will do that
    // once the snapshot entry has been committed and we know that a quorum of
    // the cluster's logs have advanced beyond the snapshot index.

    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      context.transition(Follower.class);
    }
  }

  /**
   * Handles a poll request.
   */
  protected void handlePoll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(null);
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getCurrentTerm()) {
      request.respond(context.getCurrentTerm(), false);
    }
    // If the requesting candidate is ourself then always vote for ourself. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate().equals(context.cluster.config().getLocalMember())) {
      context.setLastVotedFor(context.cluster.config().getLocalMember());
      request.respond(context.getCurrentTerm(), true);
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.cluster.config().getMembers().contains(request.candidate())) {
      request.respond(context.getCurrentTerm(), false);
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
      // If the log is empty then vote for the candidate.
      if (context.log.isEmpty()) {
        context.setLastVotedFor(request.candidate());
        request.respond(context.getCurrentTerm(), true);
      } else {
        // Otherwise, load the last entry in the log. The last entry should be
        // at least as up to date as the candidates entry and term.
        long lastIndex = context.log.lastIndex();
        Entry entry = context.log.getEntry(lastIndex);
        long lastTerm = entry.term();
        if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
          context.setLastVotedFor(request.candidate());
          request.respond(context.getCurrentTerm(), true);
        } else {
          context.setLastVotedFor(null);
          request.respond(context.getCurrentTerm(), false);
        }
      }
    }
    // In this case, we've already voted for someone else.
    else {
      request.respond(context.getCurrentTerm(), false);
    }
  }

  /**
   * Handles a submit request.
   */
  protected void handleSubmit(SubmitRequest request) {
    request.respond("Not the leader");
  }

  @Override
  public void destroy() {
    context.cluster.localMember().protocol().server().pingCallback(null);
    context.cluster.localMember().protocol().server().syncCallback(null);
    context.cluster.localMember().protocol().server().installCallback(null);
    context.cluster.localMember().protocol().server().pollCallback(null);
    context.cluster.localMember().protocol().server().submitCallback(null);
  }

}
