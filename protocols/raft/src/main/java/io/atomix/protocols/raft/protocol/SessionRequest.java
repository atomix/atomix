// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Base session request.
 * <p>
 * This is the base request for session-related requests. Many client requests are handled within the
 * context of a {@link #session()} identifier.
 */
public abstract class SessionRequest extends AbstractRaftRequest {
  protected final long session;

  protected SessionRequest(long session) {
    this.session = session;
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long session() {
    return session;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().isAssignableFrom(object.getClass())) {
      return false;
    }

    SessionRequest request = (SessionRequest) object;
    return request.session == session;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .toString();
  }

  /**
   * Session request builder.
   */
  public abstract static class Builder<T extends Builder<T, U>, U extends SessionRequest> extends AbstractRaftRequest.Builder<T, U> {
    protected long session;

    /**
     * Sets the session ID.
     *
     * @param session The session ID.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code session} is less than 0
     */
    @SuppressWarnings("unchecked")
    public T withSession(long session) {
      checkArgument(session > 0, "session must be positive");
      this.session = session;
      return (T) this;
    }

    @Override
    protected void validate() {
      checkArgument(session > 0, "session must be positive");
    }
  }
}
