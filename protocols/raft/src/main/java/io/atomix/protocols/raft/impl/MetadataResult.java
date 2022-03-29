// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.impl;

import io.atomix.primitive.session.SessionMetadata;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Metadata result.
 */
public final class MetadataResult {
  final Set<SessionMetadata> sessions;

  MetadataResult(Set<SessionMetadata> sessions) {
    this.sessions = sessions;
  }

  /**
   * Returns the session metadata.
   *
   * @return The session metadata.
   */
  public Set<SessionMetadata> sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("sessions", sessions)
        .toString();
  }
}
