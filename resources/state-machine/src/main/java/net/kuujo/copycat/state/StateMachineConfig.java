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

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.util.serializer.Serializer;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * State machine configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineConfig extends StateLogConfig {
  private static final String STATE_MACHINE_STATE_TYPE = "state-type";
  private static final String STATE_MACHINE_INITIAL_STATE = "initial-state";

  private static final String DEFAULT_CONFIGURATION = "state-machine-defaults";
  private static final String CONFIGURATION = "state-machine";

  public StateMachineConfig() {
    super(CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public StateMachineConfig(Map<String, Object> config) {
    super(config, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  public StateMachineConfig(String resource) {
    super(resource, CONFIGURATION, DEFAULT_CONFIGURATION);
  }

  protected StateMachineConfig(StateMachineConfig config) {
    super(config);
  }

  @Override
  public StateMachineConfig copy() {
    return new StateMachineConfig(this);
  }

  /**
   * Sets the status machine status type.
   *
   * @param stateType The status machine status interface.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setStateType(String stateType) {
    try {
      setStateType(Class.forName(Assert.isNotNull(stateType, "stateType")));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load state type", e);
    }
  }

  /**
   * Sets the status machine status type.
   *
   * @param stateType The status machine status type.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setStateType(Class<?> stateType) {
    this.config = config.withValue(STATE_MACHINE_STATE_TYPE, ConfigValueFactory.fromAnyRef(Assert.arg(stateType, Assert.isNotNull(stateType, "stateType").isInterface(), "state type must be an interface").getName()));
  }

  /**
   * Returns the status machine status type.
   *
   * @return The status machine status type.
   * @throws net.kuujo.copycat.util.ConfigurationException If the configured class is not found
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getStateType() {
    try {
      return config.hasPath(STATE_MACHINE_STATE_TYPE) ? (Class<T>) Class.forName(config.getString(STATE_MACHINE_STATE_TYPE)) : null;
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load state type", e);
    }
  }

  /**
   * Sets the status machine status type, returning the status machine configuration for method chaining.
   *
   * @param stateType The status machine status type.
   * @return The status machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withStateType(String stateType) {
    setStateType(stateType);
    return this;
  }

  /**
   * Sets the status machine status type, returning the status machine configuration for method chaining.
   *
   * @param stateType The status machine status type.
   * @return The status machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withStateType(Class<?> stateType) {
    setStateType(stateType);
    return this;
  }

  /**
   * Sets the status machine's initial status.
   *
   * @param initialState The status machine's initial status.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setInitialState(String initialState) {
    try {
      setInitialState(Class.forName(Assert.isNotNull(initialState, "initialState")));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load initial state", e);
    }
  }

  /**
   * Sets the status machine's initial status.
   *
   * @param initialState The status machine's initial status.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setInitialState(Class<?> initialState) {
    Assert.isNotNull(initialState, "initialState");
    Assert.arg(initialState, !initialState.isInterface() && !initialState.isEnum() && !initialState.isAnonymousClass() && !Modifier.isAbstract(initialState.getModifiers()), "state implementations must be concrete classes");
    this.config = config.withValue(STATE_MACHINE_INITIAL_STATE, ConfigValueFactory.fromAnyRef(initialState.getName()));
  }

  /**
   * Returns the status machine's initial status.
   *
   * @return The status machine's initial status.
   * @throws net.kuujo.copycat.util.ConfigurationException If the configured class is not found
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getInitialState() {
    try {
      return config.hasPath(STATE_MACHINE_INITIAL_STATE) ? (Class<T>) Class.forName(config.getString(STATE_MACHINE_INITIAL_STATE)) : null;
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load initial state", e);
    }
  }

  /**
   * Sets the status machine's initial status, returning the status machine configuration for method chaining.
   *
   * @param initialState The status machine's initial status.
   * @return The status machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withInitialState(String initialState) {
    setInitialState(initialState);
    return this;
  }

  /**
   * Sets the status machine's initial status, returning the status machine configuration for method chaining.
   *
   * @param initialState The status machine's initial status.
   * @return The status machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withInitialState(Class<?> initialState) {
    setInitialState(initialState);
    return this;
  }

  @Override
  public StateMachineConfig withSerializer(String serializer) {
    setSerializer(serializer);
    return this;
  }

  @Override
  public StateMachineConfig withSerializer(Class<? extends Serializer> serializer) {
    setSerializer(serializer);
    return this;
  }

  @Override
  public StateMachineConfig withSerializer(Serializer serializer) {
    setSerializer(serializer);
    return this;
  }

  @Override
  public StateMachineConfig withElectionTimeout(long electionTimeout) {
    setElectionTimeout(electionTimeout);
    return this;
  }

  @Override
  public StateMachineConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  @Override
  public StateMachineConfig withHeartbeatInterval(long heartbeatInterval) {
    setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  @Override
  public StateMachineConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  @Override
  public StateMachineConfig withReplicas(String... replicas) {
    setReplicas(Arrays.asList(replicas));
    return this;
  }

  @Override
  public StateMachineConfig withReplicas(Collection<String> replicas) {
    setReplicas(replicas);
    return this;
  }

  @Override
  public StateMachineConfig addReplica(String replica) {
    super.addReplica(replica);
    return this;
  }

  @Override
  public StateMachineConfig removeReplica(String replica) {
    super.removeReplica(replica);
    return this;
  }

  @Override
  public StateMachineConfig clearReplicas() {
    super.clearReplicas();
    return this;
  }

  @Override
  public StateMachineConfig withLog(Log log) {
    setLog(log);
    return this;
  }

  @Override
  public StateMachineConfig withDefaultConsistency(String consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  @Override
  public StateMachineConfig withDefaultConsistency(Consistency consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

}
