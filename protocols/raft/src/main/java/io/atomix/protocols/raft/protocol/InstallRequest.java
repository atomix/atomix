/*
 * Copyright 2015-present Open Networking Foundation
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
 * limitations under the License
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.utils.misc.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Server snapshot installation request.
 * <p>
 * Snapshot installation requests are sent by the leader to a follower when the follower indicates
 * that its log is further behind than the last snapshot taken by the leader. Snapshots are sent
 * in chunks, with each chunk being sent in a separate install request. As requests are received by
 * the follower, the snapshot is reconstructed based on the provided {@link #chunkOffset()} and other
 * metadata. The last install request will be sent with {@link #complete()} being {@code true} to
 * indicate that all chunks of the snapshot have been sent.
 */
public class InstallRequest extends AbstractRaftRequest {

  /**
   * Returns a new install request builder.
   *
   * @return A new install request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long term;
  private final MemberId leader;
  private final long index;
  private final long timestamp;
  private final int version;
  private final int offset;
  private final byte[] data;
  private final boolean complete;

  public InstallRequest(long term, MemberId leader, long index, long timestamp, int version, int offset, byte[] data, boolean complete) {
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.timestamp = timestamp;
    this.version = version;
    this.offset = offset;
    this.data = data;
    this.complete = complete;
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
  public MemberId leader() {
    return leader;
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long snapshotIndex() {
    return index;
  }

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public long snapshotTimestamp() {
    return timestamp;
  }

  /**
   * Returns the snapshot version.
   *
   * @return the snapshot version
   */
  public int snapshotVersion() {
    return version;
  }

  /**
   * Returns the offset of the snapshot chunk.
   *
   * @return The offset of the snapshot chunk.
   */
  public int chunkOffset() {
    return offset;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  /**
   * Returns a boolean value indicating whether this is the last chunk of the snapshot.
   *
   * @return Indicates whether this request is the last chunk of the snapshot.
   */
  public boolean complete() {
    return complete;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, offset, complete, data);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InstallRequest) {
      InstallRequest request = (InstallRequest) object;
      return request.term == term
          && request.leader == leader
          && request.index == index
          && request.offset == offset
          && request.complete == complete
          && Arrays.equals(request.data, data);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("leader", leader)
        .add("index", index)
        .add("timestamp", timestamp)
        .add("version", version)
        .add("offset", offset)
        .add("data", ArraySizeHashPrinter.of(data))
        .add("complete", complete)
        .toString();
  }

  /**
   * Snapshot request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, InstallRequest> {
    private long term;
    private MemberId leader;
    private long index;
    private long timestamp;
    private int version;
    private int offset;
    private byte[] data;
    private boolean complete;

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
    public Builder withLeader(MemberId leader) {
      this.leader = checkNotNull(leader, "leader cannot be null");
      return this;
    }

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(long index) {
      checkArgument(index >= 0, "index must be positive");
      this.index = index;
      return this;
    }

    /**
     * Sets the request timestamp.
     *
     * @param timestamp The request timestamp.
     * @return The request builder.
     */
    public Builder withTimestamp(long timestamp) {
      checkArgument(timestamp >= 0, "timestamp must be positive");
      this.timestamp = timestamp;
      return this;
    }

    /**
     * Sets the request version.
     *
     * @param version the request version
     * @return the request builder
     */
    public Builder withVersion(int version) {
      checkArgument(version > 0, "version must be positive");
      this.version = version;
      return this;
    }

    /**
     * Sets the request offset.
     *
     * @param offset The request offset.
     * @return The request builder.
     */
    public Builder withOffset(int offset) {
      checkArgument(offset >= 0, "offset must be positive");
      this.offset = offset;
      return this;
    }

    /**
     * Sets the request snapshot bytes.
     *
     * @param data The snapshot bytes.
     * @return The request builder.
     */
    public Builder withData(byte[] data) {
      this.data = checkNotNull(data, "data cannot be null");
      return this;
    }

    /**
     * Sets whether the request is complete.
     *
     * @param complete Whether the snapshot is complete.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withComplete(boolean complete) {
      this.complete = complete;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(term > 0, "term must be positive");
      checkNotNull(leader, "leader cannot be null");
      checkArgument(index >= 0, "index must be positive");
      checkArgument(offset >= 0, "offset must be positive");
      checkNotNull(data, "data cannot be null");
    }

    /**
     * @throws IllegalStateException if member is null
     */
    @Override
    public InstallRequest build() {
      validate();
      return new InstallRequest(term, leader, index, timestamp, version, offset, data, complete);
    }
  }

}
