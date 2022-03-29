// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.protocol;

import io.atomix.primitive.Replication;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive descriptor.
 */
public class PrimitiveDescriptor {
  private final String name;
  private final String type;
  private final byte[] config;
  private final int backups;
  private final Replication replication;

  public PrimitiveDescriptor(String name, String type, byte[] config, int backups, Replication replication) {
    this.name = name;
    this.type = type;
    this.config = config;
    this.backups = backups;
    this.replication = replication;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the primitive type name.
   *
   * @return the primitive type name
   */
  public String type() {
    return type;
  }

  /**
   * Returns the primitive service configuration.
   *
   * @return the primitive service configuration
   */
  public byte[] config() {
    return config;
  }

  /**
   * Returns the number of backups.
   *
   * @return the number of backups
   */
  public int backups() {
    return backups;
  }

  /**
   * Returns the primitive service replication strategy.
   *
   * @return the primitive service replication strategy
   */
  public Replication replication() {
    return replication;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("backups", backups)
        .add("replication", replication)
        .toString();
  }
}
