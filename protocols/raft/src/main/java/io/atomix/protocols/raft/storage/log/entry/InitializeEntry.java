// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

/**
 * Indicates a leader change has occurred.
 * <p>
 * The {@code InitializeEntry} is logged by a leader at the beginning of its term to indicate that
 * a leadership change has occurred. Importantly, initialize entries are logged with a {@link #timestamp() timestamp}
 * which can be used by server state machines to reset session timeouts following leader changes. Initialize entries
 * are always the first entry to be committed at the start of a leader's term.
 */
public class InitializeEntry extends TimestampedEntry {
  public InitializeEntry(long term, long timestamp) {
    super(term, timestamp);
  }
}
