// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Stores a client keep-alive request.
 */
public class KeepAliveEntry extends TimestampedEntry {
  private final long[] sessionIds;
  private final long[] commandSequences;
  private final long[] eventIndexes;

  public KeepAliveEntry(long term, long timestamp, long[] sessionIds, long[] commandSequences, long[] eventIndexes) {
    super(term, timestamp);
    this.sessionIds = sessionIds;
    this.commandSequences = commandSequences;
    this.eventIndexes = eventIndexes;
  }

  /**
   * Returns the session identifiers.
   *
   * @return The session identifiers.
   */
  public long[] sessionIds() {
    return sessionIds;
  }

  /**
   * Returns the command sequence numbers.
   *
   * @return The command sequence numbers.
   */
  public long[] commandSequenceNumbers() {
    return commandSequences;
  }

  /**
   * Returns the event indexes.
   *
   * @return The event indexes.
   */
  public long[] eventIndexes() {
    return eventIndexes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
        .add("commandSequences", ArraySizeHashPrinter.of(commandSequences))
        .add("eventIndexes", ArraySizeHashPrinter.of(eventIndexes))
        .toString();
  }
}
