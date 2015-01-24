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

import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.state.internal.DefaultStateMachine;
import net.kuujo.copycat.util.internal.Assert;
import net.kuujo.copycat.protocol.Consistency;

import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * State machine configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateMachineConfig extends ResourceConfig<StateMachineConfig> {
  public static final String STATE_MACHINE_STATE_TYPE = "state-type";
  public static final String STATE_MACHINE_INITIAL_STATE = "initial-state";
  public static final String STATE_MACHINE_DEFAULT_CONSISTENCY = "consistency";

  private static final String DEFAULT_STATE_MACHINE_DEFAULT_CONSISTENCY = "default";

  public StateMachineConfig() {
    super();
  }

  public StateMachineConfig(Map<String, Object> config) {
    super(config);
  }

  protected StateMachineConfig(StateMachineConfig config) {
    super(config);
  }

  @Override
  public StateMachineConfig copy() {
    return new StateMachineConfig(this);
  }

  /**
   * Sets the state machine state type.
   *
   * @param stateType The state machine state interface.
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
   * Sets the state machine state type.
   *
   * @param stateType The state machine state type.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setStateType(Class<?> stateType) {
    put(STATE_MACHINE_STATE_TYPE, Assert.arg(stateType, Assert.isNotNull(stateType, "stateType").isInterface(), "state type must be an interface").getName());
  }

  /**
   * Returns the state machine state type.
   *
   * @return The state machine state type.
   * @throws net.kuujo.copycat.util.ConfigurationException If the configured class is not found
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getStateType() {
    try {
      return containsKey(STATE_MACHINE_STATE_TYPE) ? (Class<T>) Class.forName(get(STATE_MACHINE_STATE_TYPE)) : null;
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load state type", e);
    }
  }

  /**
   * Sets the state machine state type, returning the state machine configuration for method chaining.
   *
   * @param stateType The state machine state type.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withStateType(String stateType) {
    setStateType(stateType);
    return this;
  }

  /**
   * Sets the state machine state type, returning the state machine configuration for method chaining.
   *
   * @param stateType The state machine state type.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withStateType(Class<?> stateType) {
    setStateType(stateType);
    return this;
  }

  /**
   * Sets the state machine's initial state.
   *
   * @param initialState The state machine's initial state.
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
   * Sets the state machine's initial state.
   *
   * @param initialState The state machine's initial state.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public void setInitialState(Class<?> initialState) {
    Assert.isNotNull(initialState, "initialState");
    Assert.arg(initialState, !initialState.isInterface() && !initialState.isEnum() && !initialState.isAnonymousClass() && !Modifier.isAbstract(initialState.getModifiers()), "state implementations must be concrete classes");
    put(STATE_MACHINE_INITIAL_STATE, initialState.getName());
  }

  /**
   * Returns the state machine's initial state.
   *
   * @return The state machine's initial state.
   * @throws net.kuujo.copycat.util.ConfigurationException If the configured class is not found
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> getInitialState() {
    try {
      return containsKey(STATE_MACHINE_INITIAL_STATE) ? (Class<T>) Class.forName(get(STATE_MACHINE_INITIAL_STATE)) : null;
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("Failed to load initial state", e);
    }
  }

  /**
   * Sets the state machine's initial state, returning the state machine configuration for method chaining.
   *
   * @param initialState The state machine's initial state.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws net.kuujo.copycat.util.ConfigurationException If the given class is not found
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withInitialState(String initialState) {
    setInitialState(initialState);
    return this;
  }

  /**
   * Sets the state machine's initial state, returning the state machine configuration for method chaining.
   *
   * @param initialState The state machine's initial state.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the {@code stateType} is {@code null}
   * @throws java.lang.IllegalArgumentException If the given class is not a valid interface
   */
  public StateMachineConfig withInitialState(Class<?> initialState) {
    setInitialState(initialState);
    return this;
  }

  /**
   * Sets the state machine read consistency.
   *
   * @param consistency The state machine read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(String consistency) {
    put(STATE_MACHINE_DEFAULT_CONSISTENCY, Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString());
  }

  /**
   * Sets the state machine read consistency.
   *
   * @param consistency The state machine read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setDefaultConsistency(Consistency consistency) {
    put(STATE_MACHINE_DEFAULT_CONSISTENCY, Assert.isNotNull(consistency, "consistency").toString());
  }

  /**
   * Returns the state machine read consistency.
   *
   * @return The state machine read consistency.
   */
  public Consistency getDefaultConsistency() {
    return Consistency.parse(get(STATE_MACHINE_DEFAULT_CONSISTENCY, DEFAULT_STATE_MACHINE_DEFAULT_CONSISTENCY));
  }

  /**
   * Sets the state machine read consistency, returning the configuration for method chaining.
   *
   * @param consistency The state machine read consistency.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateMachineConfig withDefaultConsistency(String consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  /**
   * Sets the state machine read consistency, returning the configuration for method chaining.
   *
   * @param consistency The state machine read consistency.
   * @return The state machine configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public StateMachineConfig withDefaultConsistency(Consistency consistency) {
    setDefaultConsistency(consistency);
    return this;
  }

  @Override
  public CoordinatedResourceConfig resolve(ClusterConfig cluster) {
    return new StateLogConfig(toMap())
      .resolve(cluster)
      .withResourceFactory(context -> new DefaultStateMachine<>(context, getStateType(), getInitialState()));
  }

}
