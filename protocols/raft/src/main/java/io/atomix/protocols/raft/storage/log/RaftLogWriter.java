// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.DelegatingJournalWriter;
import io.atomix.storage.journal.SegmentedJournalWriter;

/**
 * Raft log writer.
 */
public class RaftLogWriter extends DelegatingJournalWriter<RaftLogEntry> {
  public RaftLogWriter(SegmentedJournalWriter<RaftLogEntry> writer, RaftLog log) {
    super(writer);
  }
}
