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
package net.kuujo.raft.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.kuujo.raft.Command;
import net.kuujo.raft.log.CommandEntry;
import net.kuujo.raft.log.ConfigurationEntry;
import net.kuujo.raft.log.Entry;
import net.kuujo.raft.log.NoOpEntry;
import net.kuujo.raft.log.Entry.Type;
import net.kuujo.raft.protocol.PingRequest;
import net.kuujo.raft.protocol.PingResponse;
import net.kuujo.raft.protocol.PollRequest;
import net.kuujo.raft.protocol.SubmitRequest;
import net.kuujo.raft.protocol.SyncRequest;
import net.kuujo.raft.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * A leader state.
 *
 * @author Jordan Halterman
 */
public class Leader extends State {
  private long pingTimer;
  private Set<String> members;
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap;
  private final Set<Majority> majorities = new HashSet<>();

  @Override
  public void startUp(Handler<Void> doneHandler) {
    // Create a set of replica references in the cluster.
    members = new HashSet<>();
    replicas = new ArrayList<>();
    for (String address : context.members()) {
      if (!address.equals(context.address())) {
        members.add(address);
        replicas.add(replicaMap.put(address, new Replica(address)));
      }
    }

    // Set self as the current leader.
    context.setCurrentLeader(context.address());

    // Set up a timer for pinging cluster members.
    pingTimer = vertx.setPeriodic(context.syncInterval(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        for (Replica replica : replicas) {
          replica.update();
        }
      }
    });

    // Commit a NOOP entry to the log.
    context.setLastApplied(log.appendEntry(new NoOpEntry(context.currentTerm())));

    // Immediately update all nodes.
    for (Replica replica : replicas) {
      replica.update();
    }

    doneHandler.handle((Void) null);
  }

  @Override
  public void configure(Set<String> members) {
    // Store the new configuration and add any members to the current replica sets.
    for (String address : context.members()) {
      if (!address.equals(context.address())) {
        this.members.add(address);
        if (!replicaMap.containsKey(address)) {
          replicas.add(replicaMap.put(address, new Replica(address)));
        }
      }
    }

    context.setLastApplied(log.appendEntry(new ConfigurationEntry(context.currentTerm(), members)));

    for (Replica replica : replicas) {
      replica.update();
    }
  }

  @Override
  public void handlePing(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.setCurrentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void handleSync(SyncRequest request) {
    // If a newer term was provided by the request then continue with the sync
    // and step down once the log has been updated.
    if (request.term() > context.currentTerm()) {
      if (request.prevLogIndex() > 0 && !log.containsEntry(request.prevLogIndex()) || log.entry(request.prevLogIndex()).term() != request.prevLogTerm()) {
        request.reply(context.currentTerm(), false);
      }
      else {
        if (log.containsEntry(request.prevLogIndex() + 1) && log.entry(request.prevLogIndex() + 1).term() != request.entry().term()) {
          log.removeAfter(request.prevLogIndex());
        }
        log.appendEntry(request.entry());
        if (request.commit() > context.commitIndex()) {
          updateCommitIndex(Math.min(request.commit(), log.lastIndex()));
        }
        request.reply(context.currentTerm(), true);
      }
      context.setCurrentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
  }

  @Override
  public void handlePoll(PollRequest request) {
    if (request.term() < context.currentTerm()) {
      request.reply(context.currentTerm(), false);
    }
    else if ((context.votedFor() == null || context.votedFor().equals(request.candidate()))
        && request.lastLogIndex() >= log.lastIndex() && request.lastLogTerm() >= log.lastEntry().term()) {
      request.reply(context.currentTerm(), true);
    }
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void handleSubmit(final SubmitRequest request) {
    // If the state machine supports this command, apply the command to the state
    // machine based on the command type.
    if (stateMachine.hasCommand(request.command().command())) {
      Command.Type commandType = stateMachine.getCommandType(request.command().command());
      // If this is a read command then we need to contact a majority of the cluster
      // to ensure that the information is not stale. Once we've determined that
      // this node is the most up-to-date, we can simply apply the command to the
      // state machine and return the result without replicating the log.
      if (commandType.equals(Command.Type.READ) || commandType.equals(Command.Type.READ_WRITE)) {
        readMajority(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            request.reply(stateMachine.applyCommand(request.command()));
          }
        });
      }
      else {
        final long index = log.appendEntry(new CommandEntry(context.currentTerm(), request.command()));
        writeMajority(index, new Handler<Void>() {
          @Override
          public void handle(Void arg0) {
            JsonObject result = stateMachine.applyCommand(request.command());
            context.setLastApplied(index);
            request.reply(result);
          }
        });
      }
    }
    // If the state machine doesn't support the command then simply return an error.
    else {
      request.error("Command unsupported.");
    }
  }

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    if (pingTimer > 0) {
      vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }
    // Cancel all majority vote attempts.
    for (Majority majority : majorities) {
      majority.cancel();
    }
    doneHandler.handle((Void) null);
  }

  private void updateCommitIndex(long commitIndex) {
    // If any configuration entries are being committed, update the cluster members.
    if (context.commitIndex() < commitIndex) {
      long prevCommitIndex = context.commitIndex();
      for (Entry entry : log.entries(prevCommitIndex+1, commitIndex)) {
        if (entry.type().equals(Type.CONFIGURATION)) {
          // Remove the configuration from the list of configurations.
          context.removeConfig();
  
          // Recreate the members set.
          members = new HashSet<>();
          for (String address : context.members()) {
            if (!address.equals(context.address())) {
              members.add(address);
            }
          }
  
          // Iterate through replicas and remove any replicas that were removed from the cluster.
          Iterator<Replica> iterator = replicas.iterator();
          while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (!members.contains(replica.address)) {
              replica.updating = false;
              iterator.remove();
              replicaMap.remove(replica.address);
            }
          }
        }
      }
  
      // Finally, set the new commit index. This will be sent to replicas to
      // instruct them to apply the entries to their state machines.
      context.setCommitIndex(commitIndex);
      log.floor(Math.min(context.commitIndex(), context.lastApplied()));
    }
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    Collections.sort(replicas, new Comparator<Replica>() {
      @Override
      public int compare(Replica o1, Replica o2) {
        return Long.compare(o1.matchIndex, o2.matchIndex);
      }
    });

    int middle = (int) Math.ceil(replicas.size() / 2);
    updateCommitIndex(replicas.get(middle).matchIndex);
  }

  /**
   * Replicates the index to a majority of the cluster.
   */
  private void writeMajority(final long index, final Handler<Void> doneHandler) {
    final Majority majority = new Majority(members);
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).sync(index, new Handler<Void>() {
            @Override
            public void handle(Void _) {
              majority.succeed(address);
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle((Void) null);
      }
    });
  }

  /**
   * Pings a majority of the cluster.
   */
  private void readMajority(final Handler<Void> doneHandler) {
    final Majority majority = new Majority(members);
    majorities.add(majority);
    majority.start(new Handler<String>() {
      @Override
      public void handle(final String address) {
        if (replicaMap.containsKey(address)) {
          replicaMap.get(address).ping(new Handler<Void>() {
            @Override
            public void handle(Void _) {
              majority.succeed(address);
            }
          });
        }
      }
    }, new Handler<Boolean>() {
      @Override
      public void handle(Boolean succeeded) {
        majorities.remove(majority);
        doneHandler.handle((Void) null);
      }
    });
  }

  private class Replica {
    private final String address;
    private long nextIndex = log.lastIndex() + 1;
    private long matchIndex;
    private boolean updating;
    private Map<Long, Handler<Void>> updateHandlers = new HashMap<>();

    private Replica(String address) {
      this.address = address;
    }

    private void update() {
      if (updating) {
        return;
      }
      updating = true;

      if (nextIndex <= log.lastIndex()) {
        sync();
      }
      else {
        ping();
      }
    }

    private void ping() {
      endpoint.ping(address, new PingRequest(context.currentTerm(), context.address()),
      context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
        @Override
        public void handle(AsyncResult<PingResponse> result) {
          // If the replica returned a newer term then step down.
          if (result.succeeded() && result.result().term() > context.currentTerm()) {
            context.transition(StateType.FOLLOWER);
          }
          updating = false;
        }
      });
    }

    private void ping(final Handler<Void> doneHandler) {
      endpoint.ping(address, new PingRequest(context.currentTerm(), context.address()),
      context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
        @Override
        public void handle(AsyncResult<PingResponse> result) {
          // If the replica returned a newer term then step down.
          if (result.succeeded() && result.result().term() > context.currentTerm()) {
            context.transition(StateType.FOLLOWER);
          }
          doneHandler.handle((Void) null);
        }
      });
    }

    private void sync(long index, Handler<Void> doneHandler) {
      if (nextIndex <= log.lastIndex()) {
        updateHandlers.put(index, doneHandler);
        sync();
      }
      else {
        doneHandler.handle((Void) null);
      }
    }

    private void sync() {
      if (updating) {
        System.out.println(Leader.this.context.address() + ": Synchronizing log entry " + nextIndex + ": " + log.entry(nextIndex).type().getName() + " to " + address);
        final long index = nextIndex;
        endpoint.sync(address, new SyncRequest(context.currentTerm(), context.address(), nextIndex-1, (nextIndex - 1) >= 0 ? log.entry(nextIndex-1).term() : -1, log.entry(nextIndex), context.commitIndex()),
            context.electionTimeout() / 2, new Handler<AsyncResult<SyncResponse>>() {
          @Override
          public void handle(AsyncResult<SyncResponse> result) {
            // If the request failed then wait to retry.
            if (result.failed()) {
              updating = false;
            }
            // If the follower returned a higher term then step down.
            else if (result.result().term() > context.currentTerm()) {
              context.setCurrentTerm(result.result().term());
              context.transition(StateType.FOLLOWER);
            }
            // If the log entry failed, decrement next index and retry.
            else if (!result.result().success()) {
              if (nextIndex - 1 == -1) {
                updating = false;
                context.transition(StateType.FOLLOWER);
              }
              else {
                nextIndex--;
                sync();
              }
            }
            // If the log entry succeeded, continue to the next entry.
            else {
              nextIndex++;
              matchIndex++;
              if (updateHandlers.containsKey(index)) {
                updateHandlers.remove(index).handle((Void) null);
              }
              checkCommits();
              if (nextIndex <= log.lastIndex()) {
                sync();
              }
              else {
                updating = false;
              }
            }
          }
        });
      }
    }

    @Override
    public String toString() {
      return address;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Replica && ((Replica) other).address.equals(address);
    }

    @Override
    public int hashCode() {
      return address.hashCode();
    }
  }

}
