// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.RaftMember;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Member configuration change request.
 */
public class ReconfigureRequest extends ConfigurationRequest {

  /**
   * Returns a new reconfigure request builder.
   *
   * @return A new reconfigure request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long index;
  private final long term;

  public ReconfigureRequest(RaftMember member, long index, long term) {
    super(member);
    this.index = index;
    this.term = term;
  }

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReconfigureRequest) {
      ReconfigureRequest request = (ReconfigureRequest) object;
      return request.index == index && request.term == term && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("term", term)
        .add("member", member)
        .toString();
  }

  /**
   * Reconfigure request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<Builder, ReconfigureRequest> {
    private long index = -1;
    private long term = -1;

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
     * Sets the request term.
     *
     * @param term The request term.
     * @return The request builder.
     */
    public Builder withTerm(long term) {
      checkArgument(term >= 0, "term must be positive");
      this.term = term;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(index >= 0, "index must be positive");
      checkArgument(term >= 0, "term must be positive");
    }

    @Override
    public ReconfigureRequest build() {
      validate();
      return new ReconfigureRequest(member, index, term);
    }
  }
}
