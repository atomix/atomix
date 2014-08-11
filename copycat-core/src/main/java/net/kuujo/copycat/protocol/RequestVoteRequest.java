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
 * Request vote request.<p>
 *
 * Request vote requests are simple vote requests between one node and another.
 * When a node becomes a candidate, it will send poll requests to each
 * other node in the cluster. The poll request includes information about
 * the candidate node's log which will be used to determine whether a
 * requestee will vote for the candidate.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RequestVoteRequest extends Request {
  private static final long serialVersionUID = -5282035829185619414L;
  private long term;
  private String candidate;
  private long lastLogIndex;
  private long lastLogTerm;

  public RequestVoteRequest() {
    super(null);
  }

  public RequestVoteRequest(Object id, long term, String candidate, long lastLogIndex, long lastLogTerm) {
    super(id);
    this.term = term;
    this.candidate = candidate;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
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
   * Returns the candidate's address.
   * 
   * @return The candidate's address.
   */
  public String candidate() {
    return candidate;
  }

  /**
   * Returns the candidate's last log index.
   * 
   * @return The candidate's last log index.
   */
  public long lastLogIndex() {
    return lastLogIndex;
  }

  /**
   * Returns the candidate's last log term.
   * 
   * @return The candidate's last log term.
   */
  public long lastLogTerm() {
    return lastLogTerm;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, candidate=%s, lastLogIndex=%d, lastLogTerm=%d]", getClass().getSimpleName(), term, candidate, lastLogIndex, lastLogTerm);
  }

}
