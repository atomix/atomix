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
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.DelegatingJournalWriter;
import io.atomix.storage.journal.SegmentedJournalWriter;

/**
 * Raft log writer.
 */
public class RaftLogWriter extends DelegatingJournalWriter<RaftLogEntry> {
  private final SegmentedJournalWriter<RaftLogEntry> writer;
  private final RaftLog log;

  public RaftLogWriter(SegmentedJournalWriter<RaftLogEntry> writer, RaftLog log) {
    super(writer);
    this.writer = writer;
    this.log = log;
  }

  /**
   * Resets the head of the log to the given index.
   *
   * @param index the index to which to reset the head of the log
   */
  public void reset(long index) {
    writer.reset(index);
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  public void commit(long index) {
    if (index > log.getCommitIndex()) {
      log.setCommitIndex(index);
      if (log.isFlushOnCommit()) {
        flush();
      }
    }
  }

  @Override
  public void truncate(long index) {
    if (index < log.getCommitIndex()) {
      throw new IndexOutOfBoundsException("Cannot truncate committed index: " + index);
    }
    super.truncate(index);
  }
}
