// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import io.atomix.primitive.session.SessionMetadata;
import io.atomix.protocols.raft.RaftError;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster metadata response.
 */
public class MetadataResponse extends AbstractRaftResponse {

  /**
   * Returns a new metadata response builder.
   *
   * @return A new metadata response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Set<SessionMetadata> sessions;

  public MetadataResponse(Status status, RaftError error, Set<SessionMetadata> sessions) {
    super(status, error);
    this.sessions = sessions;
  }

  /**
   * Returns the session metadata.
   *
   * @return Session metadata.
   */
  public Set<SessionMetadata> sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("sessions", sessions)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }

  /**
   * Metadata response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, MetadataResponse> {
    private Set<SessionMetadata> sessions;

    /**
     * Sets the session metadata.
     *
     * @param sessions The client metadata.
     * @return The metadata response builder.
     */
    public Builder withSessions(SessionMetadata... sessions) {
      return withSessions(Arrays.asList(checkNotNull(sessions, "sessions cannot be null")));
    }

    /**
     * Sets the session metadata.
     *
     * @param sessions The client metadata.
     * @return The metadata response builder.
     */
    public Builder withSessions(Collection<SessionMetadata> sessions) {
      this.sessions = new HashSet<>(checkNotNull(sessions, "sessions cannot be null"));
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        checkNotNull(sessions, "sessions cannot be null");
      }
    }

    @Override
    public MetadataResponse build() {
      validate();
      return new MetadataResponse(status, error, sessions);
    }
  }
}
