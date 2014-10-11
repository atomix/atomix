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

/**
 * Ping request.<p>
 *
 * The ping request is periodically sent to followers by the leader. Pinging
 * followers notifies them that the leader is still alive and ensures their
 * logs remain in sync with the leader's logs.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PingRequest extends Request {
  private static final long serialVersionUID = 8870779945535041744L;
  private long term;
  private String leader;
  private long logIndex;
  private long logTerm;
  private long commitIndex;

  public PingRequest() {
    super(null);
  }

  public PingRequest(Object id, long term, String leader, long logIndex, long logTerm, long commitIndex) {
    super(id);
    this.term = term;
    this.leader = leader;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
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
   * Returns the index of the entry in the leader's log.
   * 
   * @return The index of the entry in the leader's log.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the term of the entry in the leader's log.
   * 
   * @return The term of the entry in the leader's log.
   */
  public long logTerm() {
    return logTerm;
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
    if (object instanceof PingRequest) {
      PingRequest request = (PingRequest) object;
      return request.id().equals(id()) && request.term == term && request.leader.equals(leader) && request.logIndex == logIndex && request.logTerm == logTerm;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id().hashCode();
    hashCode = 37 * hashCode + (int)(term ^ (term >>> 32));
    hashCode = 37 * hashCode + leader.hashCode();
    hashCode = 37 * hashCode + (int)(logIndex ^ (logIndex >>> 32));
    hashCode = 37 * hashCode + (int)(logTerm ^ (logTerm >>> 32));
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, term=%d, leader=%s, logIndex=%d, logTerm=%d, commitIndex=%d]", getClass().getSimpleName(), id(), term, leader, logIndex, logTerm, commitIndex);
  }

}
