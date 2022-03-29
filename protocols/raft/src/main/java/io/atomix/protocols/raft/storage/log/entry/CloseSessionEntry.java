// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Close session entry.
 */
public class CloseSessionEntry extends SessionEntry {
  private final boolean expired;
  private final boolean delete;

  public CloseSessionEntry(long term, long timestamp, long session, boolean expired, boolean delete) {
    super(term, timestamp, session);
    this.expired = expired;
    this.delete = delete;
  }

  /**
   * Returns whether the session is expired.
   *
   * @return Indicates whether the session is expired.
   */
  public boolean expired() {
    return expired;
  }

  /**
   * Returns whether to delete the service.
   *
   * @return whether to delete the service
   */
  public boolean delete() {
    return delete;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .add("session", session)
        .add("expired", expired)
        .add("delete", delete)
        .toString();
  }
}
