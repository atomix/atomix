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
package io.atomix.protocols.phi.protocol;

import io.atomix.protocols.phi.FailureDetectionEvent;
import io.atomix.utils.Identifier;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Failure detection heartbeat message.
 */
public class HeartbeatMessage<T extends Identifier> {
  private T source;
  private FailureDetectionEvent.State state;

  public HeartbeatMessage(T source, FailureDetectionEvent.State state) {
    this.source = source;
    this.state = state != null ? state : FailureDetectionEvent.State.ACTIVE;
  }

  /**
   * Returns the message source.
   *
   * @return the message source
   */
  public T source() {
    return source;
  }

  /**
   * Returns the message state.
   *
   * @return the message state
   */
  public FailureDetectionEvent.State state() {
    return state;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("source", source)
        .add("state", state)
        .toString();
  }
}