/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import java.util.HashSet;
import java.util.Set;

/**
 * Copycat state model.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateModel {

  /**
   * Returns a new state model builder.
   *
   * @return A new state model builder.
   */
  static Builder builder() {
    return new Builder();
  }

  private State initialState;
  private Set<State> states = new HashSet<>();

  /**
   * Returns the initial state.
   *
   * @return The initial state.
   */
  public State getInitialState() {
    return initialState;
  }

  /**
   * Returns a set of all states.
   *
   * @return A set of all states.
   */
  public Set<State> getStates() {
    return states;
  }

  /**
   * State model builder.
   */
  public static class Builder {
    private final StateModel model = new StateModel();

    private Builder() {
    }

    /**
     * Sets the initial state.
     *
     * @param state The initial state machine state.
     * @return The state model builder.
     */
    public Builder withInitialState(State state) {
      model.initialState = state;
      model.states.add(state);
      return this;
    }

    /**
     * Adds a state to the state model.
     *
     * @param state The state to add to the state model.
     * @return The state model builder.
     */
    public Builder addState(State state) {
      model.states.add(state);
      return this;
    }

    /**
     * Builds the state model.
     *
     * @return The state model.
     */
    public StateModel build() {
      return model;
    }

  }

}
