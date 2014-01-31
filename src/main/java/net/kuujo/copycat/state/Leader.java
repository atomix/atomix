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
package net.kuujo.copycat.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.NoOpEntry;
import net.kuujo.copycat.log.Entry.Type;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * A leader state.
 * 
 * @author Jordan Halterman
 */
public class Leader extends BaseState implements Observer {
  private static final Logger logger = LoggerFactory.getLogger(Leader.class);
  private final StateLock lock = new StateLock();
  private long pingTimer;
  private final Set<Long> periodicTimers = new HashSet<>();
  private List<Replica> replicas;
  private Map<String, Replica> replicaMap = new HashMap<>();
  private final Set<Majority> majorities = new HashSet<>();
  private final List<Set<String>> configs = new ArrayList<>();

  @Override
  public void startUp(final Handler<Void> doneHandler) {
    // Create a set of replica references in the cluster.
    members = new HashSet<>();
    replicas = new ArrayList<>();
    for (String address : config.getMembers()) {
      if (!address.equals(context.address())) {
        members.add(address);
        Replica replica = new Replica(address);
        replicaMap.put(address, replica);
        replicas.add(replica);
      }
    }

    // Set self as the current leader.
    context.currentLeader(context.address());

    // Set up a timer for pinging cluster members.
    pingTimer = vertx.setPeriodic(context.heartbeatInterval(), new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        for (Replica replica : replicas) {
          replica.update();
        }
      }
    });

    // Immediately commit a NOOP entry to the log. If the commit fails
    // then we periodically retry appending the entry until successful.
    // The leader cannot start until this no-op entry has been
    // successfully appended.
    periodicRetry(100, new Handler<Handler<Boolean>>() {
      @Override
      public void handle(final Handler<Boolean> doneHandler) {
        log.appendEntry(new NoOpEntry(context.currentTerm()), new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              context.lastApplied(result.result());

              // Once the no-op entry has been appended, immediately update
              // all nodes.
              for (Replica replica : replicas) {
                replica.update();
              }

              // Observe the cluster configuration for changes.
              config.addObserver(Leader.this);
              doneHandler.handle(true);
            }
            else {
              doneHandler.handle(false);
            }
          }
        });
      }
    });
  }

  @Override
  public void update(Observable config, Object arg) {
    clusterChanged((ClusterConfig) config);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(ClusterConfig config) {
    // Append the new configuration to a list of cluster configurations. The
    // cluster configurations list makes up a combined list of nodes in the
    // cluster
    // during configuration changes. Once the new configuration has been
    // committed
    // the old configuration will be removed from the list.
    configs.add(config.getMembers());

    // With the list of cluster configurations, we can create a comprehensive
    // set of cluster members.
    Set<String> combinedMembers = new HashSet<>();
    for (Set<String> membersSet : configs) {
      combinedMembers.addAll(membersSet);
    }

    // Now, add any new members to the local members set. This set makes up
    // the total combined current cluster membership.
    // Note that we only *add* members and don't *remove* any members because
    // removal of cluster members should *only* take place once the new
    // configuration
    // has been replicated and committed.
    for (String address : combinedMembers) {
      // We're creating a set of *remote* members, so skip the member if it
      // is referencing the current replica.
      if (!address.equals(context.address())) {
        this.members.add(address);
        if (!replicaMap.containsKey(address)) {
          Replica replica = new Replica(address);
          replicaMap.put(address, replica);
          replicas.add(replica);
        }
      }
    }

    // Append a new configuration entry to the log. Each time a cluster
    // configuration
    // changes the *combined* configuration is appended to the log and
    // replicated
    // to other nodes. Once each configuration is replicated and committed, the
    // old configuration will be removed and a new combined configuration will
    // be created.
    log.appendEntry(new ConfigurationEntry(context.currentTerm(), combinedMembers), new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        context.lastApplied(result.result());
      }
    });
  }

  /**
   * Periodically retries a handler.
   */
  private void periodicRetry(final long delay, final Handler<Handler<Boolean>> handler) {
    periodicTimers.add(vertx.setPeriodic(delay, new Handler<Long>() {
      @Override
      public void handle(final Long timerID) {
        handler.handle(new Handler<Boolean>() {
          @Override
          public void handle(Boolean succeeded) {
            if (succeeded) {
              vertx.cancelTimer(timerID);
              periodicTimers.remove(timerID);
            }
          }
        });
      }
    }));
  }

  @Override
  public void ping(PingRequest request) {
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
      context.transition(StateType.FOLLOWER);
    }
    request.reply(context.currentTerm());
  }

  @Override
  public void sync(final SyncRequest request) {
    // If a newer term was provided by the request then sync as normal
    // and then step down as leader.
    if (request.term() > context.currentTerm()) {
      // Acquire a lock that prevents the local log from being modified
      // during the sync.
      lock.acquire(new Handler<Void>() {
        @Override
        public void handle(Void _) {
          doSync(request, new Handler<AsyncResult<Boolean>>() {
            @Override
            public void handle(AsyncResult<Boolean> result) {
              // Once the new entries have been synchronized, step down.
              context.currentLeader(request.leader());
              context.currentTerm(request.term());
              context.transition(StateType.FOLLOWER);

              // Reply to the request.
              if (result.failed()) {
                request.error(result.cause());
              }
              else {
                request.reply(context.currentTerm(), result.result());
              }

              // Release the log lock.
              lock.release();
            }
          });
        }
      });
    }
    // Otherwise, we must have received some sync request from a node
    // that *thinks* it's the leader, but boy does it have another thing coming!
    else {
      request.reply(context.currentTerm(), false);
    }
  }

  @Override
  public void poll(final PollRequest request) {
    doPoll(request, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          request.error(result.cause());
        }
        else {
          request.reply(context.currentTerm(), result.result());
        }
      }
    });
  }

  @Override
  public void submit(final SubmitRequest request) {
    // If this is a read command then we need to contact a majority of the
    // cluster
    // to ensure that the information is not stale. Once we've determined that
    // this node is the most up-to-date, we can simply apply the command to
    // the
    // state machine and return the result without replicating the log.
    if (request.command().type() != null && request.command().type().isReadOnly()) {
      if (context.requireReadMajority()) {
        readMajority(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            try {
              request.reply(stateMachine.applyCommand(request.command()));
            }
            catch (Exception e) {
              request.error(e.getMessage());
            }
          }
        });
      }
      else {
        try {
          request.reply(stateMachine.applyCommand(request.command()));
        }
        catch (Exception e) {
          request.error(e.getMessage());
        }
      }
    }
    // Otherwise, for write commands or for commands for which a type was not
    // explicitly provided the entry must be replicated on a
    // majority of the cluster prior to responding to the request.
    else {
      // Append a new command entry to the log.
      log.appendEntry(new CommandEntry(context.currentTerm(), request.command()), new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.failed()) {
            request.error(result.cause());
          }
          else {
            // Replicate the log entry to a majority of the cluster.
            final long index = result.result();
            if (context.requireWriteMajority()) {
              writeMajority(index, new Handler<Void>() {
                @Override
                public void handle(Void arg0) {
                  try {
                    Object output = stateMachine.applyCommand(request.command());
                    context.lastApplied(index);
                    request.reply(output);
                  }
                  catch (Exception e) {
                    request.error(e.getMessage());
                  }
                }
              });
            }
            else {
              try {
                Object output = stateMachine.applyCommand(request.command());
                context.lastApplied(index);
                request.reply(output);
              }
              catch (Exception e) {
                request.error(e.getMessage());
              }
            }
          }
        }
      });
    }
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

  @Override
  public void shutDown(Handler<Void> doneHandler) {
    // Cancel the ping timer.
    if (pingTimer > 0) {
      vertx.cancelTimer(pingTimer);
      pingTimer = 0;
    }

    // Cancel any periodic retry timers.
    Iterator<Long> iterator = periodicTimers.iterator();
    while (iterator.hasNext()) {
      vertx.cancelTimer(iterator.next());
      iterator.remove();
    }

    // Cancel all majority input attempts.
    for (Majority majority : majorities) {
      majority.cancel();
    }

    // Stop observing the cluster configuration.
    config.deleteObserver(this);
    doneHandler.handle((Void) null);
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
   * Updates the commit index and processes new commits.
   * 
   * When log entries are replicated on enough (a majority of) nodes to be
   * committed, we need to check for configuration entries that have been
   * committed. Configuration changes work using a two-phase approach where a
   * combination of the old and new configurations are first entered into the
   * log and replicated and, once committed, the previous configuration is
   * removed.
   */
  private void updateCommitIndex(final long commitIndex) {
    // If there are new entries to be committed then process the entries.
    if (context.commitIndex() < commitIndex) {
      long prevCommitIndex = context.commitIndex();
      log.entries(prevCommitIndex + 1, commitIndex, new Handler<AsyncResult<List<Entry>>>() {
        @Override
        public void handle(AsyncResult<List<Entry>> result) {
          if (result.succeeded()) {
            // Iterate through entries to be committed. If any of the
            // entries are
            // configuration entries then remove the configuration which
            // they replaced
            // from the cluster configuration and update cluster membership.
            for (Entry entry : result.result()) {
              if (entry.type().equals(Type.CONFIGURATION)) {
                // Remove the configuration from the list of configurations.
                if (!configs.isEmpty()) {
                  configs.remove(0);
                }

                // Create a new combined set of cluster members.
                Set<String> combinedMembers = new HashSet<>();
                for (Set<String> membersSet : configs) {
                  combinedMembers.addAll(membersSet);
                }

                // Recreate the members set.
                members = new HashSet<>();
                for (String address : combinedMembers) {
                  if (!address.equals(context.address())) {
                    members.add(address);
                  }
                }

                // Iterate through replicas and remove any replicas that
                // were removed from the cluster.
                Iterator<Replica> iterator = replicas.iterator();
                while (iterator.hasNext()) {
                  Replica replica = iterator.next();
                  if (!members.contains(replica.address)) {
                    replica.syncing = false;
                    iterator.remove();
                    replicaMap.remove(replica.address);
                  }
                }
              }
            }

            // Finally, set the new commit index. This will be sent to
            // replicas to
            // instruct them to apply the entries to their state machines.
            context.commitIndex(commitIndex);
            log.floor(Math.min(context.commitIndex(), context.lastApplied()), new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {

              }
            });
          }
        }
      });
    }
  }

  /**
   * A replica reference.
   */
  private class Replica {
    private final String address;
    private Long nextIndex;
    private long matchIndex;
    private boolean syncing;
    private Map<Long, Handler<Void>> updateHandlers = new HashMap<>();
    private long lastPingTime;
    private long lastSyncTime;

    private Replica(String address) {
      this.address = address;
    }

    /**
     * Sends an update to the replica.
     */
    private void update() {
      // Don't send an update if the replica is already being updated.
      // Instead, simply ping the replica to prevent an election timeout.
      // This helps ensure that we only attempt to sync any given replica
      // once at a time.
      if (syncing) {
        ping();
      }
      else {
        syncing = true;

        // Load the last log index.
        log.lastIndex(new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            if (result.succeeded()) {
              // If the next index has not yet been set then initialize it
              // to the last log index plus one.
              if (nextIndex == null) {
                nextIndex = result.result() + 1;
              }
              // If there are log entries to be replicated then sync the
              // replica.
              if (nextIndex <= result.result()) {
                sync();
              }
              // Otherwise, ping the replica to prevent an election timeout.
              else {
                ping();
              }
            }
          }
        });
      }
    }

    /**
     * Pings the replica.
     */
    private void ping() {
      final long startTime = System.currentTimeMillis();
      endpoint.ping(
          address,
          new PingRequest(context.currentTerm(), context.address()),
          context.useAdaptiveTimeouts() ? lastPingTime > 0 ? Math.round(lastPingTime * context.adaptiveTimeoutThreshold()) : context
              .electionTimeout() / 2 : context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
            @Override
            public void handle(AsyncResult<PingResponse> result) {
              // If the replica returned a newer term then step down.
              if (result.succeeded()) {
                if (result.result().term() > context.currentTerm()) {
                  context.transition(StateType.FOLLOWER);
                }
                else {
                  lastPingTime = System.currentTimeMillis() - startTime;
                }
              }
            }
          });
    }

    /**
     * Pings the replica, calling a handler once the ping is complete.
     */
    private void ping(final Handler<Void> doneHandler) {
      final long startTime = System.currentTimeMillis();
      endpoint.ping(
          address,
          new PingRequest(context.currentTerm(), context.address()),
          context.useAdaptiveTimeouts() ? lastPingTime > 0 ? Math.round(lastPingTime * context.adaptiveTimeoutThreshold()) : context
              .electionTimeout() / 2 : context.electionTimeout() / 2, new Handler<AsyncResult<PingResponse>>() {
            @Override
            public void handle(AsyncResult<PingResponse> result) {
              // If the replica returned a newer term then step down.
              if (result.succeeded()) {
                if (result.result().term() > context.currentTerm()) {
                  context.transition(StateType.FOLLOWER);
                }
                else {
                  lastPingTime = System.currentTimeMillis() - startTime;
                  doneHandler.handle((Void) null);
                }
              }
            }
          });
    }

    /**
     * Synchronizes all entries up to the given index.
     */
    private void sync(final long index, final Handler<Void> doneHandler) {
      log.lastIndex(new Handler<AsyncResult<Long>>() {
        @Override
        public void handle(AsyncResult<Long> result) {
          if (result.succeeded()) {
            if (nextIndex <= result.result()) {
              updateHandlers.put(index, doneHandler);
              sync();
            }
            else {
              doneHandler.handle((Void) null);
            }
          }
        }
      });
    }

    /**
     * Synchronizes all entries.
     */
    private void sync() {
      // If the replica is already being synced then don't sync again in order
      // to prevent creating log conflicts.
      if (syncing) {
        final long index = nextIndex;

        // Get the last log entry index.
        log.lastIndex(new Handler<AsyncResult<Long>>() {
          @Override
          public void handle(AsyncResult<Long> result) {
            // The next index must be less than or equals to the last log index.
            if (result.succeeded() && index <= result.result()) {
              // Load the log entry.
              log.entry(nextIndex, new Handler<AsyncResult<Entry>>() {
                @Override
                public void handle(AsyncResult<Entry> result) {
                  if (result.succeeded()) {
                    final Entry entry = result.result();
                    if (logger.isDebugEnabled()) {
                      logger.debug(Leader.this.context.address() + " replicating " + result.result().type().getName() + " entry "
                          + nextIndex + " to " + address);
                    }

                    // If a previous log index exists then load the previous
                    // entry and
                    // extract its metadata.
                    if (nextIndex - 1 >= 0) {
                      log.entry(nextIndex - 1, new Handler<AsyncResult<Entry>>() {
                        @Override
                        public void handle(AsyncResult<Entry> result) {
                          if (result.succeeded()) {
                            final long lastLogTerm = result.result().term();
                            doSync(index, entry, nextIndex - 1, lastLogTerm, context.commitIndex());
                          }
                        }
                      });
                    }
                    // If no previous log entry exists then previous log entry
                    // and term are -1.
                    else {
                      doSync(index, entry, nextIndex - 1, -1, context.commitIndex());
                    }
                  }
                }
              });
            }
          }
        });
      }
    }

    /**
     * Synchronizes a single entry to the replica.
     */
    private void doSync(final long index, Entry entry, long prevLogIndex, long prevLogTerm, long commitIndex) {
      final long startTime = System.currentTimeMillis();
      endpoint.sync(
          address,
          new SyncRequest(context.currentTerm(), context.address(), prevLogIndex, prevLogTerm, entry, commitIndex),
          context.useAdaptiveTimeouts() ? lastSyncTime > 0 ? Math.round(lastSyncTime * context.adaptiveTimeoutThreshold()) : context
              .electionTimeout() / 2 : context.electionTimeout() / 2, new Handler<AsyncResult<SyncResponse>>() {
            @Override
            public void handle(AsyncResult<SyncResponse> result) {
              // If the request failed then wait to retry.
              if (result.failed()) {
                syncing = false;
              }
              // If the follower returned a higher term then step down.
              else if (result.result().term() > context.currentTerm()) {
                context.currentTerm(result.result().term());
                context.transition(StateType.FOLLOWER);
              }
              // If the log entry failed, decrement next index and retry.
              else if (!result.result().success()) {
                if (nextIndex - 1 == -1) {
                  syncing = false;
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
                log.lastIndex(new Handler<AsyncResult<Long>>() {
                  @Override
                  public void handle(AsyncResult<Long> result) {
                    if (result.succeeded() && nextIndex <= result.result()) {
                      sync();
                    }
                    else {
                      syncing = false;
                    }
                  }
                });
              }
              lastSyncTime = System.currentTimeMillis() - startTime;
            }
          });
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
