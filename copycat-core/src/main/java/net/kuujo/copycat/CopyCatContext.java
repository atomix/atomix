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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.impl.DefaultCluster;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.MemoryLog;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.registry.Registry;
import net.kuujo.copycat.registry.impl.ConcurrentRegistry;

/**
 * CopyCat replica context.<p>
 *
 * The <code>CopyCatContext</code> is the primary API for creating
 * and running a CopyCat replica. Given a state machine, a cluster
 * configuration, and a log, the context will communicate with other
 * nodes in the cluster, applying and replicating state machine commands.<p>
 *
 * CopyCat uses a Raft-based consensus algorithm to perform leader election
 * and state machine replication. In CopyCat, all state changes are made
 * through the cluster leader. When a cluster is started, nodes will
 * communicate with one another to elect a leader. When a command is submitted
 * to any node in the cluster, the command will be forwarded to the leader.
 * When the leader receives a command submission, it will first replicate
 * the command to its followers before applying the command to its state
 * machine and returning the result.<p>
 *
 * In order to prevent logs from growing too large, CopyCat uses snapshotting
 * to periodically compact logs. In CopyCat, snapshots are simply log
 * entries before which all previous entries are cleared. When a node first
 * becomes the cluster leader, it will first commit a snapshot of its current
 * state to its log. This snapshot can be used to get any new nodes up to date.<p>
 *
 * CopyCat supports dynamic cluster membership changes. If the {@link ClusterConfig}
 * provided to the CopyCat context is {@link java.util.Observable}, the cluster
 * leader will observe the configuration for changes. Note that cluster membership
 * changes can only occur on the leader's cluster configuration. This is because,
 * as with all state changes, cluster membership changes must go through the leader.
 * When cluster membership changes occur, the cluster leader will log and replicate
 * the configuration change just like any other state change, and it will ensure
 * that the membership change occurs in a manner that prevents a dual-majority
 * in the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCatContext {
  private static final Logger logger = Logger.getLogger(CopyCatContext.class.getCanonicalName());
  private final ClusterConfig clusterConfig;
  private final Registry registry;
  private final ClusterConfig internalConfig = new ClusterConfig();
  final Cluster cluster;
  final Log log;
  final StateMachineExecutor stateMachineExecutor;
  private final StateMachine stateMachine;
  private State state;
  private AsyncCallback<String> startCallback;
  private CopyCatConfig config;
  private String localUri;
  private String currentLeader;
  private ProtocolClient leaderClient;
  private final List<Callback<Void>> leaderConnectCallbacks = new ArrayList<>();
  private boolean leaderConnected;
  private long currentTerm;
  private String lastVotedFor;
  private long commitIndex = 0;
  private long lastApplied = 0;

  public CopyCatContext(StateMachine stateMachine) {
    this(stateMachine, new MemoryLog(), new ClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster) {
    this(stateMachine, new MemoryLog(), cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, Registry registry) {
    this(stateMachine, new MemoryLog(), cluster, new CopyCatConfig(), registry);
  }

  public CopyCatContext(StateMachine stateMachine, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this(stateMachine, new MemoryLog(), cluster, config, registry);
  }

  public CopyCatContext(StateMachine stateMachine, Log log) {
    this(stateMachine, log, new ClusterConfig(), new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster) {
    this(stateMachine, log, cluster, new CopyCatConfig());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config) {
    this(stateMachine, log, cluster, config, new ConcurrentRegistry());
  }

  public CopyCatContext(StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.log = log;
    this.config = config;
    this.registry = registry;
    this.clusterConfig = cluster;
    this.cluster = new DefaultCluster(cluster, this);
    this.stateMachine = stateMachine;
    this.stateMachineExecutor = new StateMachineExecutor(stateMachine);
  }

  /**
   * Kills the copycat instance.
   */
  void kill() {
    transition(None.class);
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
   * Returns the user-defined cluster configuration.<p>
   *
   * Note that because of the nature of CopyCat's dynamic cluster membership
   * support, the user-defined cluster configuration may temporarily differ
   * from the internal CopyCat cluster configuration. This is because when
   * the cluster configuration changes, CopyCat needs to replicate the configuration
   * change before officially committing the change in order to ensure log consistency.
   *
   * @return The user-defined cluster configuration.
   */
  public ClusterConfig cluster() {
    return clusterConfig;
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
   * Returns the context registry.<p>
   *
   * The registry can be used to register objects that can be accessed
   * by {@link net.kuujo.copycat.protocol.Protocol} and
   * {@link net.kuujo.copycat.endpoint.Endpoint} implementations.
   *
   * @return The context registry.
   */
  public Registry registry() {
    return registry;
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
  public CopyCatContext start(final AsyncCallback<String> callback) {
    // Set the local the remote internal cluster members at startup. This may
    // be overwritten by the logs once the replica has been started.
    internalConfig.setLocalMember(clusterConfig.getLocalMember());
    internalConfig.setRemoteMembers(clusterConfig.getRemoteMembers());

    localUri = clusterConfig.getLocalMember();

    transition(None.class);
    cluster.localMember().protocol().server().start(new AsyncCallback<Void>() {
      @Override
      public void call(AsyncResult<Void> result) {
        if (result.succeeded()) {
          startCallback = callback;
          transition(Follower.class);
        } else {
          callback.call(new AsyncResult<String>(result.cause()));
        }
      }
    });
    return this;
  }

  /**
   * Checks whether the start handler needs to be called.
   */
  private void checkStart() {
    if (currentLeader != null && startCallback != null) {
      startCallback.call(new AsyncResult<String>(currentLeader));
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
    cluster.localMember().protocol().server().stop(new AsyncCallback<Void>() {
      @Override
      public void call(AsyncResult<Void> result) {
        log.close();
        transition(None.class);
      }
    });
  }

  /**
   * Transitions the context to a new state.
   *
   * @param type The state to which to transition.
   */
  void transition(Class<? extends State> type) {
    if (this.state != null && type != null && type.isAssignableFrom(this.state.getClass()))
      return;
    logger.info(localUri + " transitioning to " + type.toString());
    final State oldState = state;
    try {
      state = type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      // Log the exception.
    }
    if (oldState != null) {
      oldState.destroy();
      state.init(this);
    } else {
      state.init(this);
    }
  }

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  public String leader() {
    return currentLeader;
  }

  /**
   * Returns a boolean indicating whether this node is the leader.
   *
   * @return Indicates whether this node is the leader.
   */
  public boolean isLeader() {
    return currentLeader != null && currentLeader.equals(localUri);
  }


  String getCurrentLeader() {
    return currentLeader;
  }

  CopyCatContext setCurrentLeader(String leader) {
    if (currentLeader == null || !currentLeader.equals(leader)) {
      logger.finer(String.format("Current cluster leader changed: %s", leader));
    }
    currentLeader = leader;

    // When a new leader is elected, we create a client and connect to the leader.
    // Once this node is connected to the leader we can begin submitting commands.
    if (leader == null) {
      leaderConnected = false;
      leaderClient = null;
    } else if (!isLeader()) {
      leaderConnected = false;
      leaderClient = cluster.member(currentLeader).protocol().client();
      leaderClient.connect(new AsyncCallback<Void>() {
        @Override
        public void call(AsyncResult<Void> result) {
          if (result.succeeded()) {
            leaderConnected = true;
            Iterator<Callback<Void>> iterator = leaderConnectCallbacks.iterator();
            while (iterator.hasNext()) {
              Callback<Void> callback = iterator.next();
              iterator.remove();
              callback.call((Void) null);
            }
            checkStart();
          }
        }
      });
    } else {
      leaderConnected = true;
      Iterator<Callback<Void>> iterator = leaderConnectCallbacks.iterator();
      while (iterator.hasNext()) {
        Callback<Void> callback = iterator.next();
        iterator.remove();
        callback.call((Void) null);
      }
      checkStart();
    }
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

  Object nextCorrelationId() {
    return config.getCorrelationStrategy().nextCorrelationId(this);
  }

  long startTimer(long delay, Callback<Long> callback) {
    return config.getTimerStrategy().startTimer(delay, callback);
  }

  void cancelTimer(long id) {
    if (id > 0) {
      config.getTimerStrategy().cancelTimer(id);
    }
  }

  /**
   * Submits a no-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, AsyncCallback<T> callback) {
    return submitCommand(command, new ArrayList<>(), callback);
  }

  /**
   * Submits a one-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg The command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg), callback);
  }

  /**
   * Submits a two-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1), callback);
  }

  /**
   * Submits a three-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2), callback);
  }

  /**
   * Submits a four-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3), callback);
  }

  /**
   * Submits a five-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4), callback);
  }

  /**
   * Submits a six-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param arg5 The sixth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4, arg5), callback);
  }

  /**
   * Submits a seven-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param arg5 The sixth command argument.
   * @param arg6 The seventh command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4, arg5, arg6), callback);
  }

  /**
   * Submits a eight-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param arg5 The sixth command argument.
   * @param arg6 The seventh command argument.
   * @param arg7 The eighth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7), callback);
  }

  /**
   * Submits a nine-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param arg5 The sixth command argument.
   * @param arg6 The seventh command argument.
   * @param arg7 The eighth command argument.
   * @param arg8 The ninth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8), callback);
  }

  /**
   * Submits a ten-argument command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param arg0 The first command argument.
   * @param arg1 The second command argument.
   * @param arg2 The third command argument.
   * @param arg3 The fourth command argument.
   * @param arg4 The fifth command argument.
   * @param arg5 The sixth command argument.
   * @param arg6 The seventh command argument.
   * @param arg7 The eighth command argument.
   * @param arg8 The ninth command argument.
   * @param arg9 The tenth command argument.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(String command, Object arg0, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7, Object arg8, Object arg9, AsyncCallback<T> callback) {
    return submitCommand(command, Arrays.asList(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9), callback);
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The name of the command to submit.
   * @param args An ordered list of command arguments.
   * @param callback An asynchronous callback to be called with the command result.
   * @return The CopyCat context.
   */
  public <T> CopyCatContext submitCommand(final String command, final List<Object> args, final AsyncCallback<T> callback) {
    if (currentLeader == null) {
      callback.call(new AsyncResult<T>(new CopyCatException("No leader available")));
    } else if (!leaderConnected) {
      leaderConnectCallbacks.add(new Callback<Void>() {
        @Override
        public void call(Void result) {
          leaderClient.submitCommand(new SubmitCommandRequest(nextCorrelationId(), command, args), new AsyncCallback<SubmitCommandResponse>() {
            @Override
            @SuppressWarnings("unchecked")
            public void call(AsyncResult<SubmitCommandResponse> result) {
              if (result.succeeded()) {
                if (result.value().status().equals(Response.Status.OK)) {
                  callback.call(new AsyncResult<T>((T) result.value()));
                } else {
                  callback.call(new AsyncResult<T>(result.value().error()));
                }
              } else {
                callback.call(new AsyncResult<T>(result.cause()));
              }
            }
          });
        }
      });
    } else {
      ProtocolHandler handler = currentLeader.equals(localUri) ? state : leaderClient;
      handler.submitCommand(new SubmitCommandRequest(nextCorrelationId(), command, args), new AsyncCallback<SubmitCommandResponse>() {
        @Override
        @SuppressWarnings("unchecked")
        public void call(AsyncResult<SubmitCommandResponse> result) {
          if (result.succeeded()) {
            if (result.value().status().equals(Response.Status.OK)) {
              callback.call(new AsyncResult<T>((T) result.value().result()));
            } else {
              callback.call(new AsyncResult<T>(result.value().error()));
            }
          } else {
            callback.call(new AsyncResult<T>(result.cause()));
          }
        }
      });
    }
    return this;
  }

}
