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
package io.atomix.protocols.raft.session;

import io.atomix.event.AbstractEvent;

/**
 * Raft session event.
 */
public class RaftSessionEvent extends AbstractEvent<RaftSessionEvent.Type, RaftSession> {

  /**
   * Raft session type.
   */
  public enum Type {
    /**
     * Indicates that a session has been opened by the client.
     */
    OPEN,

    /**
     * Indicates that a session has been expired by the cluster.
     */
    EXPIRE,

    /**
     * Indicates that a session has been closed by the client.
     */
    CLOSE,
  }

  public RaftSessionEvent(RaftSessionEvent.Type type, RaftSession subject) {
    super(type, subject);
  }

  public RaftSessionEvent(RaftSessionEvent.Type type, RaftSession subject, long time) {
    super(type, subject, time);
  }
}
