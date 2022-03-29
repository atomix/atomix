// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
