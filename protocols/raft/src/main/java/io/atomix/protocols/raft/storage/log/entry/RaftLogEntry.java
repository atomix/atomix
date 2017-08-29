/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.protocols.raft.storage.log.RaftLog;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Stores a state change in a {@link RaftLog}.
 */
public abstract class RaftLogEntry {
  protected final long term;

  public RaftLogEntry(long term) {
    this.term = term;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long term() {
    return term;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .toString();
  }
}
