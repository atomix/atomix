// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transaction participant info.
 */
public final class ParticipantInfo {
  private final String name;
  private final String type;
  private final String protocol;
  private final String group;

  public ParticipantInfo(String name, String type, String protocol, String group) {
    this.name = checkNotNull(name);
    this.type = checkNotNull(type);
    this.protocol = checkNotNull(protocol);
    this.group = group;
  }

  /**
   * Returns the participant name.
   *
   * @return the participant name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the participant type.
   *
   * @return the participant type
   */
  public String type() {
    return type;
  }

  /**
   * Returns the protocol type used by the participant.
   *
   * @return the protocol type used by the participant
   */
  public String protocol() {
    return protocol;
  }

  /**
   * Returns the partition group in which the participant is stored.
   *
   * @return the partition group in which the participant is stored
   */
  public String group() {
    return group;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ParticipantInfo) {
      ParticipantInfo info = (ParticipantInfo) object;
      return name.equals(info.name)
          && type.equals(info.type)
          && protocol.equals(info.protocol)
          && Objects.equals(group, info.group);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name(), type(), protocol(), group());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .add("type", type())
        .add("protocol", protocol())
        .add("group", group())
        .toString();
  }
}
