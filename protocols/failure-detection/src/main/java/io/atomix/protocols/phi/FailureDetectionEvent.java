/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.phi;

import io.atomix.event.AbstractEvent;
import io.atomix.utils.Identifier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Failure detection event.
 */
public class FailureDetectionEvent<T extends Identifier> extends AbstractEvent<FailureDetectionEvent.Type, T> {

  /**
   * Failure detection event type.
   */
  public enum Type {
    STATE_CHANGE,
  }

  /**
   * Failure detection event state.
   */
  public enum State {
    ACTIVE,
    INACTIVE,
  }

  private final State oldState;
  private final State newState;

  public FailureDetectionEvent(Type type, T subject, State oldState, State newState) {
    super(type, subject);
    this.oldState = oldState;
    this.newState = newState;
  }

  /**
   * Returns the old node state.
   *
   * @return the old node state.
   */
  public State oldState() {
    return oldState;
  }

  /**
   * Returns the new node state.
   *
   * @return the new node state.
   */
  public State newState() {
    return newState;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("subject", subject())
        .add("oldState", oldState)
        .add("newState", newState)
        .toString();
  }
}
