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
package net.kuujo.copycat.protocol.rpc;

import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Protocol append request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AppendRequest extends AbstractRequest {

  /**
   * Returns a new append request builder.
   *
   * @return A new append request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns an append request builder for an existing request.
   *
   * @param request The request to build.
   * @return The append request builder.
   */
  public static Builder builder(AppendRequest request) {
    return new Builder(request);
  }

  private long term;
  private String leader;
  private Long logIndex;
  private Long logTerm;
  private List<ByteBuffer> entries;
  private boolean firstIndex;
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
   * Returns the index of the log entry preceding the new entry.
   *
   * @return The index of the log entry preceding the new entry.
   */
  public Long logIndex() {
    return logIndex;
  }

  /**
   * Returns the term of the log entry preceding the new entry.
   *
   * @return The index of the term preceding the new entry.
   */
  public Long logTerm() {
    return logTerm;
  }

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  public List<ByteBuffer> entries() {
    return entries;
  }

  /**
   * Returns a boolean indicating whether the first entry is the first index in the log.
   *
   * @return Indicates whether the first entry is the first index in the log.
   */
  public boolean firstIndex() {
    return firstIndex;
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
    return Objects.hash(id, member, term, leader, logIndex, logTerm, entries, firstIndex, commitIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendRequest) {
      AppendRequest request = (AppendRequest) object;
      return request.id.equals(id)
        && request.member.equals(member)
        && request.term == term
        && request.leader.equals(leader)
        && request.logIndex.equals(logIndex)
        && request.logTerm.equals(logTerm)
        && request.entries.equals(entries)
        && request.firstIndex == firstIndex
        && request.commitIndex.equals(commitIndex);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, term=%d, leader=%s, logIndex=%d, logTerm=%d, entries=[%d], commitIndex=%d]", getClass().getSimpleName(), id, term, leader, logIndex, logTerm, entries.size(), commitIndex);
  }

  /**
   * Append request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, AppendRequest> {
    private Builder() {
      this(new AppendRequest());
    }

    private Builder(AppendRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term > 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     */
    public Builder withLeader(String leader) {
      request.leader = Assert.isNotNull(leader, "leader");
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The append request builder.
     */
    public Builder withLogIndex(Long index) {
      request.logIndex = Assert.index(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The append request builder.
     */
    public Builder withLogTerm(Long term) {
      request.logTerm = Assert.arg(term, term == null || term > 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     */
    public Builder withEntries(ByteBuffer... entries) {
      return withEntries(Arrays.asList(entries));
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     */
    public Builder withEntries(List<ByteBuffer> entries) {
      request.entries = Assert.isNotNull(entries, "entries");
      return this;
    }

    /**
     * Sets whether the first entry is the first index in the log.
     *
     * @param firstIndex Whether the first entry is the first index in the log.
     * @return The append request builder.
     */
    public Builder withFirstIndex(boolean firstIndex) {
      request.firstIndex = firstIndex;
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param index The request commit index.
     * @return The append request builder.
     */
    public Builder withCommitIndex(Long index) {
      request.commitIndex = Assert.index(index, index == null || index > 0, "index must be greater than zero");
      return this;
    }

    @Override
    public AppendRequest build() {
      super.build();
      Assert.isNotNull(request.leader, "leader");
      Assert.arg(request.term, request.term > 0, "term must be greater than zero");
      Assert.index(request.logIndex, request.logIndex == null || request.logIndex > 0, "index must be greater than zero");
      Assert.arg(request.logTerm, request.logTerm == null || request.logTerm > 0, "term must be greater than zero");
      Assert.arg(null, (request.logIndex == null && request.logTerm == null) || (request.logIndex != null && request.logTerm != null), "log index and term must both be null or neither be null");
      Assert.isNotNull(request.entries, "entries");
      Assert.index(request.commitIndex, request.commitIndex == null || request.commitIndex > 0, "commit index must be greater than zero");
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
