/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import java.util.List;

import net.kuujo.copycat.log.Entry;

/**
 * Sync request.<p>
 *
 * Sync requests are at the core of Raft's state machine replication
 * algorithm. Whenever a new command is applied to the leader's state
 * machine, the leader will replicate the command to other nodes in the
 * cluster using the sync request. Additionally, sync requests are sent
 * periodically to other nodes by the leader in order to ensure replicas
 * are up to date.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncRequest extends Request {
  private static final long serialVersionUID = 8870779945535041744L;
  private long term;
  private String leader;
  private long prevLogIndex;
  private long prevLogTerm;
  private List<? extends Entry> entries;
  private long commitIndex;

  public SyncRequest() {
    super(null);
  }

  public SyncRequest(Object id, long term, String leader, long prevLogIndex, long prevLogTerm, List<? extends Entry> entries, long commitIndex) {
    super(id);
    this.term = term;
    this.leader = leader;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.commitIndex = commitIndex;
  }

  /**
   * Returns the requesting node's current term.
   * 
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader address.
   * 
   * @return The leader's address.
   */
  public String leader() {
    return leader;
  }

  /**
   * Returns the index of the log entry preceding the new entry.
   * 
   * @return The index of the log entry preceding the new entry.
   */
  public long prevLogIndex() {
    return prevLogIndex;
  }

  /**
   * Returns the term of the log entry preceding the new entry.
   * 
   * @return The index of the term preceding the new entry.
   */
  public long prevLogTerm() {
    return prevLogTerm;
  }

  /**
   * Returns the log entries to append.
   * 
   * @return A list of log entries.
   */
  @SuppressWarnings("unchecked")
  public <T extends Entry> List<T> entries() {
    return (List<T>) entries;
  }

  /**
   * Returns the leader's commit index.
   * 
   * @return The leader commit index.
   */
  public long commitIndex() {
    return commitIndex;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncRequest) {
      SyncRequest request = (SyncRequest) object;
      return request.id().equals(id())
        && request.term == term
        && request.leader.equals(leader)
        && request.prevLogIndex == prevLogIndex
        && request.prevLogTerm == prevLogTerm
        && request.entries.equals(entries)
        && request.commitIndex == commitIndex;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id().hashCode();
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + leader.hashCode();
    hashCode = 37 * hashCode + (int)(prevLogIndex ^ (prevLogIndex >>> 32));
    hashCode = 37 * hashCode + (int)(prevLogTerm ^ (prevLogTerm >>> 32));
    hashCode = 37 * hashCode + entries.hashCode();
    hashCode = 37 * hashCode + (int)(commitIndex ^ (commitIndex >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, term=%d, leader=%s, prevLogIndex=%d, prevLogTerm=%d, commitIndex=%d, entries=%s]", getClass().getSimpleName(), id(), term, leader, prevLogIndex, prevLogTerm, commitIndex, entries);
  }

}
