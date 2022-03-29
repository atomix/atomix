// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base class for session-related entries.
 */
public abstract class SessionEntry extends TimestampedEntry {
  protected final long session;

  public SessionEntry(long term, long timestamp, long session) {
    super(term, timestamp);
    this.session = session;
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long session() {
    return session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .add("session", session)
        .toString();
  }
}
