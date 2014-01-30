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
package net.kuujo.mimeo.state;

import java.util.ArrayList;
import java.util.List;

import net.kuujo.mimeo.ReplicationServiceEndpoint;
import net.kuujo.mimeo.StateMachine;
import net.kuujo.mimeo.cluster.ClusterConfig;
import net.kuujo.mimeo.log.CommandEntry;
import net.kuujo.mimeo.log.Entry;
import net.kuujo.mimeo.log.Log;
import net.kuujo.mimeo.log.LogVisitor;
import net.kuujo.mimeo.log.Entry.Type;
import net.kuujo.mimeo.protocol.PingRequest;
import net.kuujo.mimeo.protocol.PollRequest;
import net.kuujo.mimeo.protocol.SubmitRequest;
import net.kuujo.mimeo.protocol.SyncRequest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;

/**
 * A node state context.
 * 
 * @author Jordan Halterman
 */
public class StateContext {
  private final String address;
  private final Vertx vertx;
  private final ReplicationServiceEndpoint endpoint;
  private final Log log;
  private final StateMachine stateMachine;
  private ClusterConfig cluster;
  private final StateFactory stateFactory = new StateFactory();
  private StateType stateType;
  private State state;
  private List<Handler<String>> transitionHandlers = new ArrayList<>();
  private long electionTimeout = 2500;
  private long heartbeatInterval = 1000;
  private boolean useAdaptiveTimeouts = true;
  private double adaptiveTimeoutThreshold = 4;
  private boolean requireWriteMajority = true;
  private boolean requireReadMajority = true;
  private String currentLeader;
  private long currentTerm;
  private String votedFor;
  private long commitIndex;
  private long lastApplied;

  public StateContext(String address, Vertx vertx, ReplicationServiceEndpoint endpoint, Log log, StateMachine stateMachine) {
    this.address = address;
    this.vertx = vertx;
    this.endpoint = endpoint;
    this.log = log;
    this.stateMachine = stateMachine;
    transition(StateType.START);
  }

  /**
   * Configures the state context.
   * 
   * @param config A cluster configuration.
   * @return The state context.
   */
  public StateContext configure(ClusterConfig config) {
    this.cluster = config;
    if (state != null) {
      state.setConfig(config);
    }
    return this;
  }

  /**
   * Transitions to a new state.
   * 
   * @param type The new state type.
   * @param doneHandler A handler to be called once the state transition is
   *          complete and a new leader has been elected.
   * @return The state context.
   */
  public StateContext transition(StateType type, Handler<String> doneHandler) {
    transitionHandlers.add(doneHandler);
    return transition(type);
  }

  /**
   * Transitions to a new state.
   * 
   * @param type The new state type.
   * @return The state context.
   */
  public StateContext transition(StateType type) {
    if (type.equals(stateType))
      return this;
    System.out.println(address() + " transitioning to " + type.getName());
    final StateType oldStateType = stateType;
    final State oldState = state;
    stateType = type;
    switch (type) {
      case START:
        state = stateFactory.createStart().setVertx(vertx).setEndpoint(endpoint).setStateMachine(stateMachine).setLog(log)
            .setConfig(cluster).setContext(this);
        break;
      case FOLLOWER:
        state = stateFactory.createFollower().setVertx(vertx).setEndpoint(endpoint).setStateMachine(stateMachine).setLog(log)
            .setConfig(cluster).setContext(this);
        break;
      case CANDIDATE:
        state = stateFactory.createCandidate().setVertx(vertx).setEndpoint(endpoint).setStateMachine(stateMachine).setLog(log)
            .setConfig(cluster).setContext(this);
        break;
      case LEADER:
        state = stateFactory.createLeader().setVertx(vertx).setEndpoint(endpoint).setStateMachine(stateMachine).setLog(log)
            .setConfig(cluster).setContext(this);
        break;
    }

    if (oldState != null) {
      if (oldStateType.equals(StateType.START)) {
        oldState.shutDown(new Handler<Void>() {
          @Override
          public void handle(Void _) {
            unregisterHandlers();
            log.init(new LogVisitor() {
              @Override
              public void applyEntry(Entry entry) {
                // If the entry type is a command, apply the entry to the state
                // machine.
                if (entry.type().equals(Type.COMMAND)) {
                  stateMachine.applyCommand(((CommandEntry) entry).command());
                }
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.succeeded()) {
                  state.startUp(new Handler<Void>() {
                    @Override
                    public void handle(Void _) {
                      registerHandlers(state);
                    }
                  });
                }
                else {
                  transition(oldStateType);
                }
              }
            });
          }
        });
      }
      else {
        oldState.shutDown(new Handler<Void>() {
          @Override
          public void handle(Void result) {
            unregisterHandlers();
            state.startUp(new Handler<Void>() {
              @Override
              public void handle(Void result) {
                registerHandlers(state);
              }
            });
          }
        });
      }
    }
    else {
      state.startUp(new Handler<Void>() {
        @Override
        public void handle(Void result) {
          registerHandlers(state);
        }
      });
    }
    return this;
  }

  private void registerHandlers(final State state) {
    endpoint.pingHandler(new Handler<PingRequest>() {
      @Override
      public void handle(PingRequest request) {
        state.ping(request);
      }
    });
    endpoint.syncHandler(new Handler<SyncRequest>() {
      @Override
      public void handle(SyncRequest request) {
        state.sync(request);
      }
    });
    endpoint.pollHandler(new Handler<PollRequest>() {
      @Override
      public void handle(PollRequest request) {
        state.poll(request);
      }
    });
    endpoint.submitHandler(new Handler<SubmitRequest>() {
      @Override
      public void handle(SubmitRequest request) {
        state.submit(request);
      }
    });
  }

  private void unregisterHandlers() {
    endpoint.pingHandler(null);
    endpoint.syncHandler(null);
    endpoint.pollHandler(null);
    endpoint.submitHandler(null);
  }

  /**
   * Returns the endpoint address.
   * 
   * @return The current endpoint address.
   */
  public String address() {
    return address;
  }

  /**
   * Returns the endpoint to which the state belongs.
   * 
   * @return The parent endpoint.
   */
  public ReplicationServiceEndpoint endpoint() {
    return endpoint;
  }

  /**
   * Returns the state log.
   * 
   * @return The state log.
   */
  public Log log() {
    return log;
  }

  /**
   * Returns the replica election timeout.
   * 
   * @return The replica election timeout.
   */
  public long electionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the leader election timeout.
   * 
   * @param timeout The leader election timeout.
   * @return The state context.
   */
  public StateContext electionTimeout(long timeout) {
    electionTimeout = timeout;
    return this;
  }

  /**
   * Returns the replica heartbeat interval.
   * 
   * @return The replica heartbeat interval.
   */
  public long heartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the replica heartbeat interval.
   * 
   * @param interval The replica heartbeat interval.
   * @return The state context.
   */
  public StateContext heartbeatInterval(long interval) {
    heartbeatInterval = interval;
    return this;
  }

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  public boolean useAdaptiveTimeouts() {
    return useAdaptiveTimeouts;
  }

  /**
   * Indicates whether the replica should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The state context.
   */
  public StateContext useAdaptiveTimeouts(boolean useAdaptive) {
    useAdaptiveTimeouts = useAdaptive;
    return this;
  }

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  public double adaptiveTimeoutThreshold() {
    return adaptiveTimeoutThreshold;
  }

  /**
   * Sets the adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The state context.
   */
  public StateContext adaptiveTimeoutThreshold(double threshold) {
    adaptiveTimeoutThreshold = threshold;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority replication is required for
   * write operations.
   * 
   * @return Indicates whether majority replication is required for write
   *         operations.
   */
  public boolean requireWriteMajority() {
    return requireWriteMajority;
  }

  /**
   * Sets whether majority replication is required for write operations.
   * 
   * @param require Indicates whether majority replication should be required
   *          for writes.
   * @return The state context.
   */
  public StateContext requireWriteMajority(boolean require) {
    requireWriteMajority = require;
    return this;
  }

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   * 
   * @return Indicates whether majority synchronization is required for read
   *         operations.
   */
  public boolean requireReadMajority() {
    return requireReadMajority;
  }

  /**
   * Sets whether majority synchronization is required for read operations.
   * 
   * @param require Indicates whether majority synchronization should be
   *          required for read operations.
   * @return The state context.
   */
  public StateContext requireReadMajority(boolean require) {
    requireReadMajority = require;
    return this;
  }

  /**
   * Returns the current leader.
   * 
   * @return The current leader.
   */
  public String currentLeader() {
    return currentLeader;
  }

  /**
   * Sets the current leader.
   * 
   * @param address The current leader.
   * @return The state context.
   */
  public StateContext currentLeader(String address) {
    currentLeader = address;
    for (Handler<String> handler : transitionHandlers) {
      handler.handle(address);
    }
    transitionHandlers.clear();
    return this;
  }

  /**
   * Returns the current term.
   * 
   * @return The current term.
   */
  public long currentTerm() {
    return currentTerm;
  }

  /**
   * Sets the current term.
   * 
   * @param term The current term.
   * @return The state context.
   */
  public StateContext currentTerm(long term) {
    currentTerm = term;
    votedFor = null;
    return this;
  }

  /**
   * Returns the address of the member last voted for.
   * 
   * @return The address of the member last voted for.
   */
  public String votedFor() {
    return votedFor;
  }

  /**
   * Sets the address of the member last voted for.
   * 
   * @param address The address of the member last voted for.
   * @return The state context.
   */
  public StateContext votedFor(String address) {
    votedFor = address;
    return this;
  }

  /**
   * Returns the current commit index.
   * 
   * @return The current commit index.
   */
  public long commitIndex() {
    return commitIndex;
  }

  /**
   * Sets the current commit index.
   * 
   * @param index The current commit index.
   * @return The state context.
   */
  public StateContext commitIndex(long index) {
    commitIndex = index;
    return this;
  }

  /**
   * Returns the last index applied to the state machine.
   * 
   * @return The last index applied to the state machine.
   */
  public long lastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last index applied to the state machine.
   * 
   * @param index The last index applied to the state machine.
   * @return The state context.
   */
  public StateContext lastApplied(long index) {
    lastApplied = index;
    return this;
  }

}
