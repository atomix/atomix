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

import java.util.ArrayDeque;
import java.util.Observable;
import java.util.Observer;
import java.util.Queue;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.StaticClusterConfig;
import net.kuujo.copycat.log.AsyncLog;
import net.kuujo.copycat.log.AsyncLogger;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.log.SnapshotEntry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

/**
 * Default replica context implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatContext implements Observer {
  private static final int MAX_QUEUE_SIZE = 1000;
  private static final Logger logger = LoggerFactory.getLogger(CopyCatContext.class);
  final Vertx vertx;
  final StateClient client;
  final AsyncLog log;
  final StateMachine stateMachine;
  final ClusterConfig cluster;
  final ClusterConfig stateCluster = new StaticClusterConfig();
  private CopyCatState stateType;
  private BaseState state;
  private Handler<AsyncResult<String>> startHandler;
  private Queue<WrappedCommand> commands = new ArrayDeque<WrappedCommand>();
  private CopyCatConfig config;
  private String currentLeader;
  private long currentTerm;
  private String lastVotedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public CopyCatContext(Vertx vertx, StateMachine stateMachine) {
    this(vertx, stateMachine, new MemoryLog(), new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(Vertx vertx, StateMachine stateMachine, ClusterConfig cluster) {
    this(vertx, stateMachine, new MemoryLog(), cluster, new CopyCatConfig());
  }

  public CopyCatContext(Vertx vertx, StateMachine stateMachine, Log log) {
    this(vertx, stateMachine, log, new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(Vertx vertx, StateMachine stateMachine, Log log, ClusterConfig cluster) {
    this(vertx, stateMachine, log, cluster, new CopyCatConfig());
  }

  public CopyCatContext(Vertx vertx, StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config) {
    this.vertx = vertx;
    this.log = new AsyncLogger(log);
    this.config = config;
    this.cluster = cluster;
    this.client = new StateClient(cluster.getLocalMember(), vertx);
    this.stateMachine = stateMachine;
    if (cluster instanceof Observable) {
      ((Observable) cluster).addObserver(this);
    }
  }

  @Override
  public void update(Observable cluster, Object arg) {
    clusterChanged((ClusterConfig) cluster);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(ClusterConfig cluster) {
    // The internal cluster configuration is allowed to change as long as there's
    // no current cluster leader. Once a leader has been elected, cluster configuration
    // changes must occur through log entries replicated by the leader.
    if (currentLeader == null) {
      stateCluster.setRemoteMembers(cluster.getRemoteMembers());
    }
  }

  /**
   * Returns the replica configuration.
   *
   * @return The replica configuration.
   */
  public CopyCatConfig config() {
    return config;
  }

  /**
   * Returns the cluster configuration.
   *
   * @return The cluster configuration.
   */
  public ClusterConfig cluster() {
    return cluster;
  }

  /**
   * Returns the underlying state machine.
   *
   * @return The underlying state machine.
   */
  public StateMachine stateMachine() {
    return stateMachine;
  }

  /**
   * Returns the local log.
   *
   * @return The local log.
   */
  public Log log() {
    return log.log();
  }

  /**
   * Starts the context.
   *
   * @return The replica context.
   */
  public CopyCatContext start() {
    return start(null);
  }

  /**
   * Starts the context.
   *
   * @param doneHandler An asynchronous handler to be called once the context
   *        has been started.
   * @return The replica context.
   */
  public CopyCatContext start(final Handler<AsyncResult<String>> doneHandler) {
    stateCluster.setQuorumSize(cluster.getQuorumSize());
    stateCluster.setLocalMember(cluster.getLocalMember());
    stateCluster.setRemoteMembers(cluster.getRemoteMembers());
    transition(CopyCatState.START);
    client.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
        } else {
          startHandler = doneHandler;
          initializeLog(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
              } else {
                transition(CopyCatState.FOLLOWER);
              }
            }
          });
        }
      }
    });
    return this;
  }

  /**
   * Initializes the log.
   */
  private void initializeLog(final Handler<AsyncResult<Void>> doneHandler) {
    log.open(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          final long commitIndex = getCommitIndex();
          log.lastIndex(new Handler<AsyncResult<Long>>() {
            @Override
            public void handle(AsyncResult<Long> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                final long lastIndex = result.result();
                if (lastIndex > 0 && lastIndex >= commitIndex) {
                  initializeLog(1, lastIndex, doneHandler);
                } else {
                  setLogHandlers();
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            }
          });
        }
      }
    });
  }

  /**
   * Initializes a single entry in the log.
   */
  private void initializeLog(final long currentIndex, final long lastIndex, final Handler<AsyncResult<Void>> doneHandler) {
    if (currentIndex <= lastIndex) {
      log.containsEntry(currentIndex, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
          } else if (result.result()) {
            log.getEntry(currentIndex, new Handler<AsyncResult<Entry>>() {
              @Override
              public void handle(AsyncResult<Entry> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                } else {
                  if (result.result().term() > currentTerm) {
                    currentTerm = result.result().term();
                  }
                  if (result.result() instanceof SnapshotEntry) {
                    if (stateMachine instanceof SnapshotInstaller) {
                      ((SnapshotInstaller) stateMachine).installSnapshot(((SnapshotEntry) result.result()).data());
                    }
                  } else if (result.result() instanceof ConfigurationEntry) {
                    cluster.setRemoteMembers(((ConfigurationEntry) result.result()).members());
                  } else if (result.result() instanceof CommandEntry) {
                    CommandEntry entry = (CommandEntry) result.result();
                    stateMachine.applyCommand(entry.command(), entry.args());
                  }
                  commitIndex++;
                  initializeLog(currentIndex+1, lastIndex, doneHandler);
                }
              }
            });
          } else {
            initializeLog(currentIndex+1, lastIndex, doneHandler);
          }
        }
      });
    } else {
      setLogHandlers();
      new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
    }
  }

  /**
   * Sets log handlers.
   */
  private void setLogHandlers() {
    log.fullHandler(new Handler<Void>() {
      @Override
      public void handle(Void _) {
        if (stateMachine instanceof SnapshotCreator) {
          logger.info("Building snapshot");
          JsonObject snapshot = ((SnapshotCreator) stateMachine).createSnapshot();
          if (snapshot != null) {
            // Replace the last entry that was applied to the state machien with
            // a snapshot of the machine state. Since the entry has been applied,
            // we can safely assume that it has been replicated to a majority of
            // the cluster.
            final long lastApplied = getLastApplied();
            log.setEntry(lastApplied, new SnapshotEntry(getCurrentTerm(), snapshot), new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  logger.error("Failed to store snapshot", result.cause());
                } else {
                  // Once the snapshot has been stored, remove all the entries prior
                  // to the last applied entry (now the snapshot). Those entries no
                  // longer contribute to the machine state since the snapshot should
                  // overwrite the state machine state.
                  log.removeBefore(lastApplied, new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> result) {
                      if (result.failed()) {
                        logger.error("Failed to clean log", result.cause());
                      } else {
                        logger.info("Successfully stored state snapshot");
                      }
                    }
                  });
                }
              }
            });
          }
        }
      }
    });
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startHandler != null) {
      new DefaultFutureResult<String>(currentLeader).setHandler(startHandler);
      startHandler = null;
    }
  }

  /**
   * Stops the context.
   *
   * @return The replica context.
   */
  public void stop() {
    stop(null);
  }

  /**
   * Stops the context.
   *
   * @param doneHandler An asynchronous handler to be called once the context
   *        has been stopped.
   * @return The replica context.
   */
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    client.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        log.close(doneHandler);
        transition(CopyCatState.START);
      }
    });
  }

  /**
   * Transitions the context to a new state.
   *
   * @param type The state to which to transition.
   */
  void transition(CopyCatState type) {
    if (type.equals(stateType))
      return;
    logger.info(cluster.getLocalMember() + " transitioning to " + type.toString());
    final BaseState oldState = state;
    stateType = type;
    switch (type) {
      case START:
        currentLeader = null;
        state = new Start(this);
        break;
      case FOLLOWER:
        state = new Follower(this);
        break;
      case CANDIDATE:
        state = new Candidate(this);
        break;
      case LEADER:
        state = new Leader(this);
        break;
    }

    final BaseState newState = state;

    if (oldState != null) {
      oldState.shutDown(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            logger.error(result.cause());
            transition(CopyCatState.START);
          } else {
            unregisterHandlers();
            newState.startUp(new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  logger.error(result.cause());
                  transition(CopyCatState.START);
                } else {
                  registerHandlers(newState);
                  checkStart();
                }
              }
            });
          }
        }
      });
    } else {
      state.startUp(new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            logger.error(result.cause());
            transition(CopyCatState.START);
          } else {
            registerHandlers(newState);
            checkStart();
          }
        }
      });
    }
  }

  /**
   * Registers client handlers.
   */
  private void registerHandlers(final BaseState state) {
    client.pingHandler(new Handler<PingRequest>() {
      @Override
      public void handle(PingRequest request) {
        state.ping(request);
      }
    });
    client.syncHandler(new Handler<SyncRequest>() {
      @Override
      public void handle(SyncRequest request) {
        state.sync(request);
      }
    });
    client.pollHandler(new Handler<PollRequest>() {
      @Override
      public void handle(PollRequest request) {
        state.poll(request);
      }
    });
    client.submitHandler(new Handler<SubmitRequest>() {
      @Override
      public void handle(SubmitRequest request) {
        state.submit(request);
      }
    });
  }

  /**
   * Unregisters client handlers.
   */
  private void unregisterHandlers() {
    client.pingHandler(null);
    client.syncHandler(null);
    client.pollHandler(null);
    client.submitHandler(null);
  }

  CopyCatState getCurrentState() {
    return stateType;
  }

  String getCurrentLeader() {
    return currentLeader;
  }

  CopyCatContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.debug(String.format("Current cluster leader changed: %s", leader));
    }
    currentLeader = leader;
    checkStart();
    checkQueue();
    return this;
  }

  long getCurrentTerm() {
    return currentTerm;
  }

  CopyCatContext setCurrentTerm(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      logger.debug(String.format("Updated current term %d", term));
      lastVotedFor = null;
    }
    return this;
  }

  String getLastVotedFor() {
    return lastVotedFor;
  }

  CopyCatContext setLastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      logger.debug(String.format("Voted for %s", candidate));
    }
    lastVotedFor = candidate;
    return this;
  }

  long getCommitIndex() {
    return commitIndex;
  }

  CopyCatContext setCommitIndex(long index) {
    commitIndex = index;
    return this;
  }

  long getLastApplied() {
    return lastApplied;
  }

  CopyCatContext setLastApplied(long index) {
    lastApplied = index;
    return this;
  }

  /**
   * Submits a command to the service.
   *
   * @param command The command to submit.
   * @param args Command arguments.
   * @param resultHandler An asynchronous handler to be called with the command result.
   * @return The replica context.
   */
  public CopyCatContext submitCommand(final String command, final JsonObject args, final Handler<AsyncResult<JsonObject>> doneHandler) {
    if (currentLeader == null) {
      if (commands.size() > MAX_QUEUE_SIZE) {
        new DefaultFutureResult<JsonObject>(new CopyCatException("Command queue full.")).setHandler(doneHandler);
      } else {
        commands.add(new WrappedCommand(command, args, doneHandler));
      }
    } else {
      client.submit(currentLeader, new SubmitRequest(command, args), new Handler<AsyncResult<SubmitResponse>>() {
        @Override
        public void handle(AsyncResult<SubmitResponse> result) {
          if (result.failed()) {
            new DefaultFutureResult<JsonObject>(result.cause()).setHandler(doneHandler);
          } else {
            new DefaultFutureResult<JsonObject>(result.result().result()).setHandler(doneHandler);
          }
        }
      });
    }
    return this;
  }

  /**
   * Checks the command queue.
   */
  private void checkQueue() {
    Queue<WrappedCommand> queue = new ArrayDeque<WrappedCommand>(commands);
    commands.clear();
    for (WrappedCommand command : queue) {
      submitCommand(command.command, command.args, command.doneHandler);
    }
    queue.clear();
  }

  /**
   * A wrapped state machine command request.
   */
  private static class WrappedCommand {
    private final String command;
    private final JsonObject args;
    private final Handler<AsyncResult<JsonObject>> doneHandler;
    private WrappedCommand(String command, JsonObject args, Handler<AsyncResult<JsonObject>> doneHandler) {
      this.command = command;
      this.args = args;
      this.doneHandler = doneHandler;
    }
  }

}
