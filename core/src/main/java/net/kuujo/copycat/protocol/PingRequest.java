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

/**
 * Protocol ping request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PingRequest extends AbstractRequest {

  /**
   * Returns a new ping request builder.
   *
   * @return A new ping request builder.
   */
  static Builder builder() {
    return new Builder();
  }

  private long term;
  private String leader;
  private long logIndex;
  private long logTerm;
  private long commitIndex;

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

  /**
   * Ping request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, PingRequest> {
    private Builder() {
      super(new PingRequest());
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The ping request builder.
     */
    public Builder withTerm(long term) {
      request.term = term;
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The ping request builder.
     */
    public Builder withLeader(String leader) {
      request.leader = leader;
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The ping request builder.
     */
    public Builder withLogIndex(long index) {
      request.logIndex = index;
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The ping request builder.
     */
    public Builder withLogTerm(long term) {
      request.logTerm = term;
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The ping request builder.
     */
    public Builder withCommitIndex(long index) {
      request.commitIndex = index;
      return this;
    }

  }

}
