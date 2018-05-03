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
