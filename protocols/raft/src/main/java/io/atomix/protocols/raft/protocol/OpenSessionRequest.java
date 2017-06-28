/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open session request.
 */
public class OpenSessionRequest extends AbstractRaftRequest {

  /**
   * Returns a new open session request builder.
   *
   * @return A new open session request builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private final MemberId member;
  private final String name;
  private final String typeName;
  private final ReadConsistency readConsistency;
  private final long timeout;

  public OpenSessionRequest(MemberId member, String name, String typeName, ReadConsistency readConsistency, long timeout) {
    this.member = member;
    this.name = name;
    this.typeName = typeName;
    this.readConsistency = readConsistency;
    this.timeout = timeout;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public MemberId member() {
    return member;
  }

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the state machine type;
   *
   * @return The state machine type.
   */
  public String typeName() {
    return typeName;
  }

  /**
   * Returns the session read consistency level.
   *
   * @return The session's read consistency.
   */
  public ReadConsistency readConsistency() {
    return readConsistency;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, typeName, timeout);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionRequest) {
      OpenSessionRequest request = (OpenSessionRequest) object;
      return request.member.equals(member)
          && request.name.equals(name)
          && request.typeName.equals(typeName)
          && request.readConsistency == readConsistency
          && request.timeout == timeout;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("node", member)
        .add("name", name)
        .add("typeName", typeName)
        .add("readConsistency", readConsistency)
        .add("timeout", timeout)
        .toString();
  }

  /**
   * Open session request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, OpenSessionRequest> {
    private MemberId member;
    private String name;
    private String typeName;
    private ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
    private long timeout;

    /**
     * Sets the client node identifier.
     *
     * @param member The client node identifier.
     * @return The open session request builder.
     * @throws NullPointerException if {@code node} is {@code null}
     */
    public Builder withMember(MemberId member) {
      this.member = checkNotNull(member, "node cannot be null");
      return this;
    }

    /**
     * Sets the state machine name.
     *
     * @param name The state machine name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code name} is {@code null}
     */
    public Builder withName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the state machine type name.
     *
     * @param typeName The state machine type name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code type} is {@code null}
     */
    public Builder withTypeName(String typeName) {
      this.typeName = checkNotNull(typeName, "typeName cannot be null");
      return this;
    }

    /**
     * Sets the session read consistency.
     *
     * @param readConsistency the session read consistency
     * @return the session request builder
     * @throws NullPointerException if the {@code readConsistency} is null
     */
    public Builder withReadConsistency(ReadConsistency readConsistency) {
      this.readConsistency = checkNotNull(readConsistency, "readConsistency cannot be null");
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The open session request builder.
     * @throws IllegalArgumentException if {@code timeout} is not positive
     */
    public Builder withTimeout(long timeout) {
      checkArgument(timeout >= 0, "timeout must be positive");
      this.timeout = timeout;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkNotNull(member, "client cannot be null");
      checkNotNull(name, "name cannot be null");
      checkNotNull(typeName, "typeName cannot be null");
      checkArgument(timeout >= 0, "timeout must be positive");
    }

    /**
     * @throws IllegalStateException is session is not positive
     */
    @Override
    public OpenSessionRequest build() {
      validate();
      return new OpenSessionRequest(member, name, typeName, readConsistency, timeout);
    }
  }
}
