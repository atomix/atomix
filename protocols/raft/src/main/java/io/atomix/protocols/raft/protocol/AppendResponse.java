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
 * limitations under the License.
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Server append entries response.
 */
public class AppendResponse extends AbstractRaftResponse {

  /**
   * Returns a new append response builder.
   *
   * @return A new append response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long term;
  private final boolean succeeded;
  private final long lastLogIndex;

  public AppendResponse(Status status, RaftError error, long term, boolean succeeded, long lastLogIndex) {
    super(status, error);
    this.term = term;
    this.succeeded = succeeded;
    this.lastLogIndex = lastLogIndex;
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
   * Returns a boolean indicating whether the append was successful.
   *
   * @return Indicates whether the append was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public long lastLogIndex() {
    return lastLogIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, succeeded, lastLogIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendResponse) {
      AppendResponse response = (AppendResponse) object;
      return response.status == status
          && response.term == term
          && response.succeeded == succeeded
          && response.lastLogIndex == lastLogIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("term", term)
          .add("succeeded", succeeded)
          .add("lastLogIndex", lastLogIndex)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }

  /**
   * Append response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, AppendResponse> {
    private long term;
    private boolean succeeded;
    private long lastLogIndex;

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The append response builder
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    public Builder withTerm(long term) {
      checkArgument(term > 0, "term must be positive");
      this.term = term;
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the append request succeeded.
     * @return The append response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      this.succeeded = succeeded;
      return this;
    }

    /**
     * Sets the last index of the replica's log.
     *
     * @param lastLogIndex The last index of the replica's log.
     * @return The append response builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    public Builder withLastLogIndex(long lastLogIndex) {
      checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
      this.lastLogIndex = lastLogIndex;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        checkArgument(term > 0, "term must be positive");
        checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
      }
    }

    /**
     * @throws IllegalStateException if status is ok and term is not positive or log index is negative
     */
    @Override
    public AppendResponse build() {
      validate();
      return new AppendResponse(status, error, term, succeeded, lastLogIndex);
    }
  }
}
