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

import net.kuujo.copycat.internal.util.Assert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Protocol sync request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SyncRequest extends AbstractRequest {

  /**
   * Returns a new sync request builder.
   *
   * @return A new sync request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a sync request builder for an existing request.
   *
   * @param request The request to build.
   * @return The sync request builder.
   */
  public static Builder builder(SyncRequest request) {
    return new Builder(request);
  }

  private long term;
  private String leader;
  private Long logIndex;
  private Long commitIndex;
  private List<ByteBuffer> entries;
  private Collection<ReplicaInfo> members;

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
   * Returns the last known log index.
   *
   * @return The last known log index.
   */
  public Long logIndex() {
    return logIndex;
  }

  /**
   * Returns the requesting node's commit index.
   *
   * @return The requesting node's commit index.
   */
  public Long commitIndex() {
    return commitIndex;
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
   * Returns the currently known membership.
   *
   * @return The currently known membership.
   */
  public Collection<ReplicaInfo> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, member, term, leader, entries);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SyncRequest) {
      SyncRequest request = (SyncRequest) object;
      return request.id.equals(id)
        && request.member.equals(member)
        && request.term == term
        && request.leader.equals(leader)
        && request.logIndex.equals(logIndex)
        && request.commitIndex.equals(commitIndex)
        && request.entries.equals(entries)
        && request.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, term=%d, leader=%s, logIndex=%s, commitIndex=%s, entries=[...]]", getClass().getSimpleName(), id, term, leader, logIndex, commitIndex);
  }

  /**
   * Sync request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, SyncRequest> {
    private Builder() {
      this(new SyncRequest());
    }

    private Builder(SyncRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The sync request builder.
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term >= 0, "term must be greater than zero");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The sync request builder.
     */
    public Builder withLeader(String leader) {
      request.leader = leader;
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    public Builder withEntries(ByteBuffer... entries) {
      return withEntries(Arrays.asList(entries));
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The sync request builder.
     */
    public Builder withEntries(List<ByteBuffer> entries) {
      request.entries = Assert.isNotNull(entries, "entries");
      return this;
    }

    /**
     * Sets the request log index.
     *
     * @param index The request log index.
     * @return The request builder.
     */
    public Builder withLogIndex(Long index) {
      request.logIndex = index;
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param commitIndex The request commit index.
     * @return The request builder.
     */
    public Builder withCommitIndex(Long commitIndex) {
      request.commitIndex = commitIndex;
      return this;
    }

    /**
     * Sets the request membership.
     *
     * @param members The request membership.
     * @return The sync request builder.
     */
    public Builder withMembers(Collection<ReplicaInfo> members) {
      request.members = Assert.isNotNull(members, "members");
      return this;
    }

    @Override
    public SyncRequest build() {
      super.build();
      Assert.arg(request.term, request.term >= 0, "term must be greater than zero");
      Assert.isNotNull(request.entries, "entries");
      Assert.isNotNull(request.members, "members");
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
