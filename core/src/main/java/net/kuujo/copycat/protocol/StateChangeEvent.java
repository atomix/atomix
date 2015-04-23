/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.Event;

/**
 * Protocol state change event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateChangeEvent implements Event {
  private final State oldState;
  private final State newState;

  public StateChangeEvent(State oldState, State newState) {
    this.oldState = oldState;
    this.newState = newState;
  }

  /**
   * Returns the old state.
   *
   * @return The old state.
   */
  public State oldState() {
    return oldState;
  }

  /**
   * Returns the new state.
   *
   * @return The new state.
   */
  public State newState() {
    return newState;
  }

}
