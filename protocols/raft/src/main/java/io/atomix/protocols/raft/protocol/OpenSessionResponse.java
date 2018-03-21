/*
 * Copyright 2017-present Open Networking Foundation
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
import io.atomix.protocols.raft.service.PropagationStrategy;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open session response.
 */
public class OpenSessionResponse extends AbstractRaftResponse {

  /**
   * Returns a new register client response builder.
   *
   * @return A new register client response builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  protected final long session;
  protected final long timeout;
  protected final int revision;
  protected final PropagationStrategy propagationStrategy;

  public OpenSessionResponse(Status status, RaftError error, long session, long timeout, int revision, PropagationStrategy propagationStrategy) {
    super(status, error);
    this.session = session;
    this.timeout = timeout;
    this.revision = revision;
    this.propagationStrategy = propagationStrategy;
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

  /**
   * Returns the final revision number.
   *
   * @return the final revision number
   */
  public int revision() {
    return revision;
  }

  /**
   * Returns the final propagation strategy.
   *
   * @return the final propagation strategy
   */
  public PropagationStrategy propagationStrategy() {
    return propagationStrategy;
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
          .add("revision", revision)
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
    private int revision;
    private PropagationStrategy propagationStrategy;

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

    /**
     * Sets the revision number.
     *
     * @param revision the revision number
     * @return the response builder
     */
    public Builder withRevision(int revision) {
      checkArgument(revision > 0, "revision must be positive");
      this.revision = revision;
      return this;
    }

    /**
     * Sets the propagation strategy.
     *
     * @param propagationStrategy the propagation strategy
     * @return the response builder
     */
    public Builder withPropagationStrategy(PropagationStrategy propagationStrategy) {
      this.propagationStrategy = checkNotNull(propagationStrategy);
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        checkArgument(session > 0, "session must be positive");
        checkArgument(timeout > 0, "timeout must be positive");
        checkArgument(revision > 0, "revision must be positive");
        checkNotNull(propagationStrategy, "propagationStrategy must not be null");
      }
    }

    @Override
    public OpenSessionResponse build() {
      validate();
      return new OpenSessionResponse(status, error, session, timeout, revision, propagationStrategy);
    }
  }
}
