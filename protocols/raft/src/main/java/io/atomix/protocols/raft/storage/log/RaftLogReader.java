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
import io.atomix.storage.journal.DelegatingJournalReader;
import io.atomix.storage.journal.SegmentedJournalReader;

/**
 * Raft log reader.
 */
public class RaftLogReader extends DelegatingJournalReader<RaftLogEntry> {

  /**
   * Raft log reader mode.
   */
  public enum Mode {

    /**
     * Reads all entries from the log.
     */
    ALL,

    /**
     * Reads committed entries from the log.
     */
    COMMITS,
  }

  private final SegmentedJournalReader<RaftLogEntry> reader;
  private final RaftLog log;
  private final Mode mode;

  public RaftLogReader(SegmentedJournalReader<RaftLogEntry> reader, RaftLog log, Mode mode) {
    super(reader);
    this.reader = reader;
    this.log = log;
    this.mode = mode;
  }

  /**
   * Returns the first index in the journal.
   *
   * @return the first index in the journal
   */
  public long getFirstIndex() {
    return reader.getFirstIndex();
  }

  /**
   * Returns the first index with the given term.
   *
   * @param term the term for which to return the first index
   * @return the first index for the given term
   */
  public long getFirstIndex(long term) {
    return log.getTermIndex().lookup(term);
  }

  @Override
  public boolean hasNext() {
    if (mode == Mode.ALL) {
      return super.hasNext();
    }

    long nextIndex = getNextIndex();
    long commitIndex = log.getCommitIndex();
    return nextIndex <= commitIndex && super.hasNext();
  }
}
