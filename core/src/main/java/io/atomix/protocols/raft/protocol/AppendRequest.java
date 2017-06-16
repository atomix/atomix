/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.journal.Indexed;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Append entries request.
 * <p>
 * Append entries requests are at the core of the replication protocol. Leaders send append requests
 * to followers to replicate and commit log entries, and followers sent append requests to passive members
 * to replicate committed log entries.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AppendRequest extends AbstractRaftRequest {

  /**
   * Returns a new append request builder.
   *
   * @return A new append request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long term;
  private final NodeId leader;
  private final long logIndex;
  private final long logTerm;
  private final List<Indexed<RaftLogEntry>> entries;
  private final long commitIndex;

  public AppendRequest(long term, NodeId leader, long logIndex, long logTerm, List<Indexed<RaftLogEntry>> entries, long commitIndex) {
    this.term = term;
    this.leader = leader;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
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
  public NodeId leader() {
    return leader;
  }

  /**
   * Returns the index of the log entry preceding the new entry.
   *
   * @return The index of the log entry preceding the new entry.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the term of the log entry preceding the new entry.
   *
   * @return The index of the term preceding the new entry.
   */
  public long logTerm() {
    return logTerm;
  }

  /**
   * Returns the log entries to append.
   *
   * @return A list of log entries.
   */
  public List<Indexed<RaftLogEntry>> entries() {
    return entries;
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
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, logIndex, logTerm, entries, commitIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendRequest) {
      AppendRequest request = (AppendRequest) object;
      return request.term == term
          && request.leader == leader
          && request.logIndex == logIndex
          && request.logTerm == logTerm
          && request.entries.equals(entries)
          && request.commitIndex == commitIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("leader", leader)
        .add("logIndex", logIndex)
        .add("logTerm", logTerm)
        .add("entries", entries.size())
        .add("commitIndex", commitIndex)
        .toString();
  }

  /**
   * Append request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, AppendRequest> {
    private long term;
    private NodeId leader;
    private long logIndex;
    private long logTerm;
    private List<Indexed<RaftLogEntry>> entries;
    private long commitIndex = -1;

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(long term) {
      checkArgument(term > 0, "term must be positive");
      this.term = term;
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code leader} is not positive
     */
    public Builder withLeader(NodeId leader) {
      this.leader = checkNotNull(leader, "leader cannot be null");
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param logIndex The request last log index.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code index} is not positive
     */
    public Builder withLogIndex(long logIndex) {
      checkArgument(logIndex >= 0, "logIndex must be positive");
      this.logIndex = logIndex;
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param logTerm The request last log term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withLogTerm(long logTerm) {
      checkArgument(logTerm >= 0, "logTerm must be positive");
      this.logTerm = logTerm;
      return this;
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     * @throws NullPointerException if {@code entries} is null
     */
    public Builder withEntries(Indexed<RaftLogEntry>... entries) {
      return withEntries(Arrays.asList(checkNotNull(entries, "entries cannot be null")));
    }

    /**
     * Sets the request entries.
     *
     * @param entries The request entries.
     * @return The append request builder.
     * @throws NullPointerException if {@code entries} is null
     */
    @SuppressWarnings("unchecked")
    public Builder withEntries(List<Indexed<RaftLogEntry>> entries) {
      this.entries = checkNotNull(entries, "entries cannot be null");
      return this;
    }

    /**
     * Adds an entry to the request.
     *
     * @param entry The entry to add.
     * @return The request builder.
     * @throws NullPointerException if {@code entry} is {@code null}
     */
    public Builder addEntry(Indexed<RaftLogEntry> entry) {
      this.entries.add(checkNotNull(entry, "entry"));
      return this;
    }

    /**
     * Sets the request commit index.
     *
     * @param commitIndex The request commit index.
     * @return The append request builder.
     * @throws IllegalArgumentException if index is not positive
     */
    public Builder withCommitIndex(long commitIndex) {
      checkArgument(commitIndex >= 0, "commitIndex must be positive");
      this.commitIndex = commitIndex;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(term > 0, "term must be positive");
      checkNotNull(leader, "leader cannot be null");
      checkArgument(logIndex >= 0, "logIndex must be positive");
      checkArgument(logTerm >= 0, "logTerm must be positive");
      checkNotNull(entries, "entries cannot be null");
      checkArgument(commitIndex >= 0, "commitIndex must be positive");
    }

    /**
     * @throws IllegalStateException if the term, log term, log index, commit index, or global index are not positive, or
     *                               if entries is null
     */
    @Override
    public AppendRequest build() {
      validate();
      return new AppendRequest(term, leader, logIndex, logTerm, entries, commitIndex);
    }
  }
}
