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

import net.kuujo.copycat.internal.util.Assert;

import java.util.Objects;

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
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a ping request builder for an existing request.
   *
   * @param request The request to build.
   * @return The ping request builder.
   */
  public static Builder builder(PingRequest request) {
    return new Builder(request);
  }

  private long term;
  private String leader;
  private Long logIndex;
  private Long logTerm;
  private Long commitIndex;

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
  public Long logIndex() {
    return logIndex;
  }

  /**
   * Returns the term of the entry in the leader's log.
   *
   * @return The term of the entry in the leader's log.
   */
  public Long logTerm() {
    return logTerm;
  }

  /**
   * Returns the leader's commit index.
   *
   * @return The leader commit index.
   */
  public Long commitIndex() {
    return commitIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, term, leader, logIndex, logTerm, commitIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PingRequest) {
      PingRequest request = (PingRequest) object;
      return request.id.equals(id)
        && request.member.equals(member)
        && request.term == term
        && request.leader.equals(leader)
        && request.logIndex.equals(logIndex)
        && request.logTerm.equals(logTerm)
        && request.commitIndex.equals(commitIndex);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, uri=%s, term=%d, leader=%s, logIndex=%d, logTerm=%d, commitIndex=%d]", getClass().getSimpleName(), id, member, term, leader, logIndex, logTerm, commitIndex);
  }

  /**
   * Ping request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, PingRequest> {
    private Builder() {
      this(new PingRequest());
    }

    private Builder(PingRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The ping request builder.
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term > 0, "term must be greater than zero");
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
      request.leader = Assert.isNotNull(leader, "leader");
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The ping request builder.
     */
    public Builder withLogIndex(Long index) {
      request.logIndex = Assert.index(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The ping request builder.
     */
    public Builder withLogTerm(Long term) {
      request.logTerm = Assert.arg(term, term == null || term > 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The ping request builder.
     */
    public Builder withCommitIndex(Long index) {
      request.commitIndex = Assert.index(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    @Override
    public PingRequest build() {
      super.build();
      Assert.isNotNull(request.leader, "leader");
      Assert.arg(request.term, request.term > 0, "term must be greater than zero");
      Assert.index(request.logIndex, request.logIndex == null || request.logIndex > 0, "index must be greater than zero");
      Assert.arg(request.logTerm, request.logTerm == null || request.logTerm > 0, "term must be greater than zero");
      Assert.arg(null, (request.logIndex == null && request.logTerm == null) || (request.logIndex != null && request.logTerm != null), "log index and term must both be null or neither be null");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
