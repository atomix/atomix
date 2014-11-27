/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.log.Entry;

import java.util.List;

/**
 * Protocol sync request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SyncRequest extends Request {

  /**
   * Returns a new sync request builder.
   *
   * @return A new sync request builder.
   */
  static Builder builder() {
    return null;
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  String leader();

  /**
   * Returns the index of the log entry preceding the new entry.
   *
   * @return The index of the log entry preceding the new entry.
   */
  long logIndex();

  /**
   * Returns the term of the log entry preceding the new entry.
   *
   * @return The index of the term preceding the new entry.
   */
  long logTerm();

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  List<Entry> entries();

  /**
   * Returns the leader's commit index.
   *
   * @return The leader commit index.
   */
  long commitIndex();

  /**
   * Sync request builder.
   */
  static interface Builder extends Request.Builder<Builder, SyncRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The sync request builder.
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The sync request builder.
     */
    Builder withLeader(String leader);

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The sync request builder.
     */
    Builder withLogIndex(long index);

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The sync request builder.
     */
    Builder withLogTerm(long term);

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    Builder withEntries(Entry... entries);

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    Builder withEntries(List<Entry> entries);

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The sync request builder.
     */
    Builder withCommitIndex(long index);

  }

}
