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

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.SnapshotEntry;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.util.serializer.Serializer;

/**
 * Base replica state implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseState {
  protected final CopyCatContext context;

  protected BaseState(CopyCatContext context) {
    this.context = context;
  }

  /**
   * Starts up the state.
   */
  void init() {
  }

  /**
   * Handles a ping request.
   * 
   * @param request The request to handle.
   */
  abstract void ping(PingRequest request);

  /**
   * Handles a sync request.
   * 
   * @param request The request to handle.
   */
  abstract void sync(SyncRequest request);

  /**
   * Handles an install request.
   *
   * @param request The request to handle.
   */
  abstract void install(InstallRequest request);

  /**
   * Handles a poll request.
   * 
   * @param request The request to handle.
   */
  abstract void poll(PollRequest request);

  /**
   * Handles a submit command request.
   * 
   * @param request The request to handle.
   */
  abstract void submit(SubmitRequest request);

  /**
   * Tears down the state.
   */
  void destroy() {
  }

  /**
   * Handles an install request.
   */
  protected boolean doInstall(final InstallRequest request) {
    if (request.term() < context.getCurrentTerm()) {
      return false;
    }

    // Get the index of the snapshot entry.
    long index = request.snapshotIndex();

    // Determine whether a partial snapshot was already written to the log.
    Entry entry = context.log.getEntry(index);
    if (entry == null) {
      // If no snapshot entry exists, create a new snapshot entry. The snapshot
      // entry should be appended at the given index. If the indexes don't match
      // then respond with a failure message. Otherwise, just respond with the
      // current term which we've already determine matches the request term.
      SnapshotEntry newEntry = new SnapshotEntry(request.term(), context.stateCluster.getMembers(), request.data(), request.complete());
      long appendIndex = context.log.appendEntry(newEntry);
      if (appendIndex != index) {
        return false;
      } else {
        return true;
      }
    }

    // If an entry already exists at the given index, ensure it's a snapshot
    // entry and matches the given snapshot request. If the entry is not a
    // snapshot entry or does not match the request information, replace the
    // entry with a new snapshot entry. Otherwise, if the snapshot entry
    // is not complete then append the given request to the entry.
    if (entry instanceof SnapshotEntry) {
      SnapshotEntry snapshotEntry = (SnapshotEntry) entry;
      if (snapshotEntry.term() != request.snapshotTerm()) {
        SnapshotEntry newEntry = new SnapshotEntry(request.term(), context.stateCluster.getMembers(), request.data(), request.complete());
        context.log.setEntry(index, newEntry);
      } else {
        byte[] newBytes = new byte[snapshotEntry.data().length + request.data().length];
        System.arraycopy(snapshotEntry.data(), 0, newBytes, 0, snapshotEntry.data().length);
        System.arraycopy(request.data(), 0, newBytes, snapshotEntry.data().length, request.data().length);
        SnapshotEntry newEntry = new SnapshotEntry(snapshotEntry.term(), context.stateCluster.getMembers(), newBytes, request.complete());
        context.log.setEntry(index, newEntry);
      }
    } else {
      SnapshotEntry newEntry = new SnapshotEntry(request.term(), context.stateCluster.getMembers(), request.data(), request.complete());
      context.log.setEntry(index, newEntry);
    }
    return true;
  }

  /**
   * Handles a sync request.
   */
  protected boolean doSync(final SyncRequest request) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getCurrentTerm()) {
      return false;
    } else {
      // Otherwise, continue on to check the log consistency.
      return checkConsistency(request);
    }
  }

  /**
   * Checks log consistency for a sync request.
   */
  private boolean checkConsistency(final SyncRequest request) {
    // If a previous log index and term were provided then check to ensure
    // that they match this node's previous log index and term.
    if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      return checkPreviousEntry(request);
    } else {
      // Otherwise, continue on to check the entry being appended.
      return appendEntries(request);
    }
  }

  /**
   * Checks that the given previous log entry of a sync request matches the
   * previous log entry of this node.
   */
  private boolean checkPreviousEntry(final SyncRequest request) {
    // Check whether the log contains an entry at prevLogIndex.
    if (context.log.containsEntry(request.prevLogIndex())) {
      // If the log entry exists then load the entry.
      // If the last log entry's term is not the same as the given
      // prevLogTerm then return false. This will cause the leader to
      // decrement this node's nextIndex and ultimately retry with the
      // leader's previous log entry so that the inconsistent entry
      // can be overwritten.
      Entry entry = context.log.getEntry(request.prevLogIndex());
      if (entry.term() != request.prevLogTerm()) {
        return false;
      } else {
        // Finally, if the log appears to be consistent then continue on
        // to remove invalid entries from the log.
        return appendEntries(request);
      }
    } else {
      // If no log entry was found at the previous log index then return false.
      // This will cause the leader to decrement this node's nextIndex and
      // ultimately retry with the leader's previous log entry.
      return false;
    }
  }

  /**
   * Appends request entries to the log.
   */
  private boolean appendEntries(final SyncRequest request) {
    return appendEntries(request.prevLogIndex(), request.entries(), request);
  }

  /**
   * Appends request entries to the log.
   */
  private boolean appendEntries(final long prevIndex, final List<Entry> entries, final SyncRequest request) {
    for (int i = (int) (prevIndex+1); i < prevIndex + entries.size(); i++) {
      // Get the entry at the current index from the log.
      Entry entry = context.log.getEntry(i);
      // If the log does not contain an entry at this index then this
      // indicates no conflict, append the new entry.
      if (entry == null) {
        context.log.appendEntry(entries.get(i));
      } else if (entry.term() != entries.get(i).term()) {
        // If the local log's equivalent entry's term does not match the
        // synced entry's term then that indicates that it came from a
        // different leader. The log must be purged of this entry and all
        // entries following it.
        context.log.removeAfter(i-1);
        context.log.appendEntry(entries.get(i));
      }
    }
    return checkApplyCommits(request);
  }

  /**
   * Checks for entries that have been committed and applies committed entries
   * to the local state machine.
   */
  private boolean checkApplyCommits(final SyncRequest request) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (request.commit() > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = context.log.lastIndex();
      context.setCommitIndex(Math.min(request.commit(), lastIndex));

      // If the updated commit index indicates that commits remain to be
      // applied to the state machine, iterate entries and apply them.
      if (context.getCommitIndex() > Math.min(context.getLastApplied(), lastIndex)) {
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
            members.remove(context.stateCluster.getLocalMember());
            context.stateCluster.setRemoteMembers(members);
          }
          context.setLastApplied(context.getLastApplied() + 1);
        }

        // Once entries have been applied check whether we need to compact the log.
        // If the log has now grown past the maximum local log size, create a
        // snapshot of the current state machine state and replace the applied
        // entries with a single snapshot entry. The snapshot is stored as a log
        // entry in order to make replication simpler if the node becomes a leader.
        if (context.log.size() > context.config().getMaxLogSize()) {
          Map<String, Object> snapshot = context.stateMachine.createSnapshot();
          byte[] bytes = Serializer.getInstance().writeValue(snapshot);
          SnapshotEntry entry = new SnapshotEntry(context.getCurrentTerm(), context.stateCluster.getMembers(), bytes, true);
          context.log.setEntry(context.getLastApplied(), entry);
          context.log.removeBefore(context.getLastApplied());
        }
        return true;
      }
    }
    return true;
  }

  /**
   * Handles a poll request.
   */
  protected boolean doPoll(final PollRequest request) {
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
    }

    // If the requesting candidate is the current node then vote for self.
    if (request.candidate().equals(context.cluster().getLocalMember())) {
      context.setLastVotedFor(request.candidate());
      return true;

    // If the requesting candidate is not a known member of the cluster (to
    // this replica) then reject the vote. This helps ensure that new cluster
    // members cannot become leader until at least a majority of the cluster
    // has been notified of their membership.
    } else if (!context.stateCluster.getMembers().contains(request.candidate())) {
      return false;
    } else {
      // If the request term is less than the current term then don't
      // vote for the candidate.
      if (request.term() < context.getCurrentTerm()) {
        return false;
      }
      // If we haven't yet voted or already voted for this candidate then check
      // that the candidate's log is at least as up-to-date as the local log.
      else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
        // It's possible that the last log index could be 0, indicating that
        // the log does not contain any entries. If that is the cases then
        // the log must *always* be at least as up-to-date as all other
        // logs.
        long lastIndex = context.log.lastIndex();
        if (lastIndex == 0) {
          context.setLastVotedFor(request.candidate());
          return true;
        } else {
          // Load the log entry to get the term. We load the log entry rather
          // than the log term to ensure that we're receiving the term from
          // the same entry as the loaded last log index.
          Entry entry = context.log.getEntry(lastIndex);

          // If the log entry was null then don't vote for the candidate.
          // This may simply result in no clear winner in the election, but
          // it's better than an imperfect leader being elected due to a
          // brief failure of the event bus.
          if (entry == null) {
            return false;
          }

          final long lastTerm = entry.term();
          if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
            context.setLastVotedFor(request.candidate());
            return true;
          } else {
            context.setLastVotedFor(null); // Reset voted for.
            return false;
          }
        }
      }
      // If we've already voted for someone else then don't vote for the
      // candidate.
      return false;
    }
  }

}
