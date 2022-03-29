// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.session;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive session metadata.
 */
public final class SessionMetadata {
  private final long id;
  private final String name;
  private final String type;

  public SessionMetadata(long id, String name, String type) {
    this.id = id;
    this.name = checkNotNull(name, "name cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
  }

  /**
   * Returns the globally unique session identifier.
   *
   * @return The globally unique session identifier.
   */
  public SessionId sessionId() {
    return SessionId.from(id);
  }

  /**
   * Returns the session name.
   *
   * @return The session name.
   */
  public String primitiveName() {
    return name;
  }

  /**
   * Returns the session type.
   *
   * @return The session type.
   */
  public String primitiveType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, name);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SessionMetadata) {
      SessionMetadata metadata = (SessionMetadata) object;
      return metadata.id == id && Objects.equals(metadata.name, name) && Objects.equals(metadata.type, type);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("name", name)
        .add("type", type)
        .toString();
  }
}
