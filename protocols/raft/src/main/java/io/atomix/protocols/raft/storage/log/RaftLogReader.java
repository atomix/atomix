// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.DelegatingJournalReader;
import io.atomix.storage.journal.SegmentedJournalReader;

/**
 * Raft log reader.
 */
public class RaftLogReader extends DelegatingJournalReader<RaftLogEntry> {
  public RaftLogReader(SegmentedJournalReader<RaftLogEntry> reader) {
    super(reader);
  }
}
