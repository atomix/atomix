// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Open session response.
 */
public class OpenSessionResponse extends AbstractRaftResponse {

  /**
   * Returns a new register client response builder.
   *
   * @return A new register client response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final long session;
  protected final long timeout;

  public OpenSessionResponse(Status status, RaftError error, long session, long timeout) {
    super(status, error);
    this.session = session;
    this.timeout = timeout;
  }

  /**
   * Returns the registered session ID.
   *
   * @return The registered session ID.
   */
  public long session() {
    return session;
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
    return Objects.hash(getClass(), error, status, session, timeout);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionResponse) {
      OpenSessionResponse response = (OpenSessionResponse) object;
      return response.status == status
          && Objects.equals(response.error, error)
          && response.session == session
          && response.timeout == timeout;
    }
    return false;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("session", session)
          .add("timeout", timeout)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, OpenSessionResponse> {
    private long session;
    private long timeout;

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegalArgumentException if {@code session} is less than 1
     */
    public Builder withSession(long session) {
      checkArgument(session > 0, "session must be positive");
      this.session = session;
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The response builder.
     */
    public Builder withTimeout(long timeout) {
      checkArgument(timeout > 0, "timeout must be positive");
      this.timeout = timeout;
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        checkArgument(session > 0, "session must be positive");
        checkArgument(timeout > 0, "timeout must be positive");
      }
    }

    @Override
    public OpenSessionResponse build() {
      validate();
      return new OpenSessionResponse(status, error, session, timeout);
    }
  }
}
