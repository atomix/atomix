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
 * A sync request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncRequest extends Request<SyncResponse> {
  private static final long serialVersionUID = 8870779945535041744L;
  private long term;
  private String leader;
  private long prevLogIndex;
  private long prevLogTerm;
  private List<Entry> entries;
  private long commit;

  public SyncRequest() {
  }

  public SyncRequest(long term, String leader, long prevLogIndex, long prevLogTerm, List<Entry> entries, long commitIndex) {
    this.term = term;
    this.leader = leader;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.entries = entries;
    this.commit = commitIndex;
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
  public List<Entry> entries() {
    return entries;
  }

  /**
   * Returns the leader's commit index.
   * 
   * @return The leader commit index.
   */
  public long commit() {
    return commit;
  }

  /**
   * Responds to the request.
   * 
   * @param term The responding node's current term.
   * @param succeeded Indicates whether the sync was successful.
   */
  public void respond(long term, boolean succeeded) {
    super.respond(new SyncResponse(term, succeeded));
  }

  /**
   * Responds to the request with an error.
   *
   * @param t The error that occurred.
   */
  public void respond(Throwable t) {
    super.respond(new SyncResponse(t));
  }

  /**
   * Responds to the request with an error message.
   *
   * @param message The error message.
   */
  public void respond(String message) {
    super.respond(new SyncResponse(message));
  }

  @Override
  public String toString() {
    return String.format("SyncRequest[term=%s, leader=%s, prevLogIndex=%s, prevLogTerm=%s, commit=%s, entries=%s]", term, leader, prevLogIndex, prevLogTerm, commit, entries);
  }

}
