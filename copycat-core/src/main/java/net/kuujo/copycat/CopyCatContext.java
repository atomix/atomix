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

import java.nio.Buffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.StaticClusterConfig;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.log.SnapshotEntry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolUri;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.util.AsyncCallback;
import net.kuujo.copycat.util.ServiceInfo;

/**
 * Default replica context implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatContext implements Observer {
  private static final int MAX_QUEUE_SIZE = 1000;
  private static final int MAX_LOG_SIZE = 10000;
  private static final Logger logger = Logger.getLogger(CopyCatContext.class.getCanonicalName());
  Protocol protocol;
  final Log log;
  final StateMachine stateMachine;
  final ClusterConfig cluster;
  final ClusterConfig stateCluster = new StaticClusterConfig();
  private CopyCatState stateType;
  private BaseState state;
  private Queue<WrappedCommand> commands = new ArrayDeque<WrappedCommand>();
  private AsyncCallback<String> startCallback;
  private CopyCatConfig config;
  private String currentLeader;
  private long currentTerm;
  private String lastVotedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public CopyCatContext(StateMachine stateMachine) {
    this(stateMachine, new MemoryLog(), new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster) {
    this(stateMachine, new MemoryLog(), cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log) {
    this(stateMachine, log, new StaticClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster) {
    this(stateMachine, log, cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config) {
    this.log = log;
    this.config = config;
    this.cluster = cluster;
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
    return log;
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
  @SuppressWarnings("unchecked")
  public CopyCatContext start(final AsyncCallback<String> callback) {
    stateCluster.setLocalMember(cluster.getLocalMember());
    stateCluster.setRemoteMembers(cluster.getRemoteMembers());

    // Set up the local protocol.
    ProtocolUri uri = new ProtocolUri(stateCluster.getLocalMember());
    ServiceInfo serviceInfo = uri.getServiceInfo();
    Class<? extends Protocol> protocolClass = serviceInfo.getProperty("class", Class.class);
    try {
      protocol = protocolClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      callback.fail(e);
      return this;
    }

    transition(CopyCatState.START);
    protocol.start(new AsyncCallback<Void>() {
      @Override
      public void complete(Void value) {
        startCallback = callback;
        initializeLog();
        transition(CopyCatState.FOLLOWER);
      }
      @Override
      public void fail(Throwable t) {
        callback.fail(t);
      }
    });
    return this;
  }

  /**
   * Initializes the log.
   */
  private void initializeLog() {
    log.open();
    long commitIndex = getCommitIndex();
    long lastIndex = log.lastIndex();
    if (lastIndex > 0 && lastIndex >= commitIndex) {
      initializeLog(1, lastIndex);
    } else {
      observeLog();
    }
  }

  /**
   * Initializes a single entry in the log.
   */
  private void initializeLog(final long currentIndex, final long lastIndex) {
    for (long i = currentIndex; i <= lastIndex; i++) {
      Entry entry = log.getEntry(i);
      if (entry != null) {
        if (entry.term() > currentTerm) {
          currentTerm = entry.term();
        }
        if (entry instanceof SnapshotEntry) {
          stateMachine.installSnapshot(((SnapshotEntry) entry).data());
        } else if (entry instanceof ConfigurationEntry) {
          Set<String> members = ((ConfigurationEntry) entry).members();
          members.remove(cluster.getLocalMember());
          cluster.setRemoteMembers(members);
        } else if (entry instanceof CommandEntry) {
          CommandEntry command = (CommandEntry) entry;
          stateMachine.applyCommand(command.command(), command.args());
        }
        commitIndex++;
      }
    }
    observeLog();
  }

  /**
   * Observes the local log for changes.
   */
  private void observeLog() {
    log.addObserver(new Observer() {
      @Override
      public void update(Observable o, Object arg) {
        Log log = (Log) o;
        if (log.size() > MAX_LOG_SIZE) {
          logger.info("Building snapshot");
          Buffer snapshot = stateMachine.createSnapshot();
          if (snapshot != null) {
            // Replace the last entry that was applied to the state machien with
            // a snapshot of the machine state. Since the entry has been applied,
            // we can safely assume that it has been replicated to a majority of
            // the cluster.
            long lastApplied = getLastApplied();
            log.setEntry(lastApplied, new SnapshotEntry(getCurrentTerm(), snapshot));

            // Once the snapshot has been stored, remove all the entries prior
            // to the last applied entry (now the snapshot). Those entries no
            // longer contribute to the machine state since the snapshot should
            // overwrite the state machine state.
            log.removeBefore(lastApplied);
            logger.info("Stored state snapshot");
          }
        }
      }
    });
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startCallback != null) {
      startCallback.complete(currentLeader);
      startCallback = null;
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
   * @param callback An asynchronous callback to be called once the context
   *        has been stopped.
   * @return The replica context.
   */
  public void stop(final AsyncCallback<Void> callback) {
    protocol.stop(new AsyncCallback<Void>() {
      @Override
      public void complete(Void value) {
        log.close();
        transition(CopyCatState.START);
      }
      @Override
      public void fail(Throwable t) {
        logger.severe("Failed to stop context");
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

    if (oldState != null) {
      oldState.destroy();
      unregisterCallbacks();
      state.init();
      registerCallbacks(state);
    } else {
      state.init();
      registerCallbacks(state);
    }
  }

  /**
   * Registers client callbacks.
   */
  private void registerCallbacks(final BaseState state) {
    protocol.pingCallback(new AsyncCallback<PingRequest>() {
      @Override
      public void complete(PingRequest request) {
        state.ping(request);
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    protocol.syncCallback(new AsyncCallback<SyncRequest>() {
      @Override
      public void complete(SyncRequest request) {
        state.sync(request);
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    protocol.pollCallback(new AsyncCallback<PollRequest>() {
      @Override
      public void complete(PollRequest request) {
        state.poll(request);
      }
      @Override
      public void fail(Throwable t) {
      }
    });
    protocol.submitCallback(new AsyncCallback<SubmitRequest>() {
      @Override
      public void complete(SubmitRequest request) {
        state.submit(request);
      }
      @Override
      public void fail(Throwable t) {
      }
    });
  }

  /**
   * Unregisters client callbacks.
   */
  private void unregisterCallbacks() {
    protocol.pingCallback(null);
    protocol.syncCallback(null);
    protocol.pollCallback(null);
    protocol.submitCallback(null);
  }

  CopyCatState getCurrentState() {
    return stateType;
  }

  String getCurrentLeader() {
    return currentLeader;
  }

  CopyCatContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
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
      logger.finer(String.format("Updated current term %d", term));
      lastVotedFor = null;
    }
    return this;
  }

  String getLastVotedFor() {
    return lastVotedFor;
  }

  CopyCatContext setLastVotedFor(String candidate) {
    if (lastVotedFor == null || !lastVotedFor.equals(candidate)) {
      logger.finer(String.format("Voted for %s", candidate));
    }
    lastVotedFor = candidate;
    return this;
  }

  long getCommitIndex() {
    return commitIndex;
  }

  CopyCatContext setCommitIndex(long index) {
    commitIndex = Math.max(commitIndex, index);
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
   * @param callback An asynchronous callback to be called with the command result.
   * @return The replica context.
   */
  public CopyCatContext submitCommand(final String command, final Map<String, Object> args, final AsyncCallback<Map<String, Object>> callback) {
    if (currentLeader == null) {
      if (commands.size() > MAX_QUEUE_SIZE) {
        callback.fail(new CopyCatException("Command queue full"));
      } else {
        commands.add(new WrappedCommand(command, args, callback));
      }
    } else {
      protocol.submit(currentLeader, new SubmitRequest(command, args), new AsyncCallback<SubmitResponse>() {
        @Override
        public void complete(SubmitResponse response) {
          if (response.status().equals(Response.Status.OK)) { 
            callback.complete(response.result());
          } else {
            callback.fail(response.error());
          }
        }
        @Override
        public void fail(Throwable t) {
          callback.fail(t);
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
    private final Map<String, Object> args;
    private final AsyncCallback<Map<String, Object>> doneHandler;
    private WrappedCommand(String command, Map<String, Object> args, AsyncCallback<Map<String, Object>> doneHandler) {
      this.command = command;
      this.args = args;
      this.doneHandler = doneHandler;
    }
  }

}
